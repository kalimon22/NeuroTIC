import os
import time
import requests
import pycozo
import json
import logging
import re
from db_utils import run_query, setup_db
from wiki_utils import WikiManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [GROUNDING] - %(message)s")

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://host.docker.internal:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3")
MAX_ITERATIONS = int(os.environ.get("MAX_ITERATIONS", "1000")) # Alto, porque escanea todo
DB_PATH = os.environ.get("DB_PATH", "data/ontology.db")

logging.info(f"Configuración: OLLAMA_HOST={OLLAMA_HOST}, OLLAMA_MODEL={OLLAMA_MODEL}")
logging.info(f"Directorios: INPUT={os.environ.get('EXTRACTOR_INPUT_DIR')}, DONE={os.environ.get('EXTRACTOR_DONE_DIR')}")

PROMPT_JSON = """Eres un experto analista documental y jurídico en ontologías.
Se te proporciona un 'Concepto' extraído de documentos reales. Tu DEBER es proveer:
1. Una descripción detallada, extensa y exhaustiva (puede ocupar varios párrafos si el concepto es complejo). Explica qué es, su función, su contexto legal o técnico y su relevancia general, sin inventar falsas relaciones con otros documentos. Puedes usar formato Markdown (negritas, listas) dentro del texto.
2. Si el concepto abarca subtipos o ejemplos reales, lístalos (Max 5).
   IMPORTANTE: Cada ejemplo debe ser un elemento separado en la lista JSON.

Concepto actual: "{concept}"

RESPONDE EXCLUSIVAMENTE CON UN OBJETO JSON VÁLIDO.
REGLAS ESTRICTAS DE JSON:
- NUNCA uses comillas triples (\\"\\"\\"). Usa solo comillas dobles (").
- ESCAPA los saltos de línea con '\\n'. No incluyas saltos de línea reales dentro de las cadenas.
- Escapa las comillas dobles internas con \\".

Esquema obligatorio:
{{
   "descripcion": "<texto extenso en formato markdown, usa '\\n\\n' para separar párrafos>",
   "instancias": ["ejemplo 1", "ejemplo 2"]
}}
"""

PROMPT_JSON_FILE = """Eres un experto analista documental y jurídico en ontologías.
Se te proporciona un 'Concepto' extraído de documentos reales, junto con los fragmentos de texto donde aparece. 
Tu DEBER es proveer:
1. Una descripción detallada, extensa y exhaustiva (puede ocupar varios párrafos). Explica qué es, su función, su contexto legal o técnico y su relevancia general, basándote ÚNICA Y EXCLUSIVAMENTE en el texto proporcionado. No inventes nada fuera de este contexto. Puedes usar formato Markdown (negritas, listas) dentro del texto.
2. Si el concepto abarca subtipos o ejemplos reales en el texto, lístalos (Max 5).
   IMPORTANTE: Cada ejemplo debe ser un elemento separado en la lista JSON.

Concepto actual: "{concept}"

TEXTO DE CONTEXTO (USAR SOLO ESTO):
\"\"\"
{context}
\"\"\"

RESPONDE EXCLUSIVAMENTE CON UN OBJETO JSON VÁLIDO.
REGLAS ESTRICTAS DE JSON:
- NUNCA uses comillas triples (\\"\\"\\"). Usa solo comillas dobles (").
- ESCAPA los saltos de línea con '\\n'. No incluyas saltos de línea reales dentro de las cadenas.
- Escapa las comillas dobles internas con \\".

Esquema obligatorio:
{{
   "descripcion": "<texto extenso en formato markdown. Si realmente no hay información en el texto proporcionado, indica 'No se encontró información suficiente en el documento.'>",
   "instancias": ["ejemplo 1", "ejemplo 2"]
}}
"""

def extract_json(text):
    """Try to find a JSON block in the text using regex."""
    # Match everything between { and } including nested braces (basic)
    match = re.search(r"(\{.*\})", text, re.DOTALL)
    if match:
        return match.group(1)
    return text

def get_next_ungrounded(db):
    try:
        query = """
        all_concepts[c] := *eav[c, _, _, _, _, _]
        all_concepts[c] := *eav[_, _, c, _, _, _]
        grounded[c] := *concept_metadata[c, _]
        ?[pending] := all_concepts[pending], not grounded[pending]
        :limit 1
        """
        res = run_query(db, query)
        if 'rows' in res and len(res['rows']) > 0:
            return res['rows'][0][0]
    except Exception as e:
        pass
    return None

def get_concept_sources(db, concept):
    try:
        query = """
        ?[source] := *eav[$c, _, _, _, source, _]
        ?[source] := *eav[_, _, $c, _, source, _]
        """
        res = run_query(db, query, {"c": concept})
        if 'rows' in res:
            srcs = list(set(row[0] for row in res['rows']))
            logging.info(f"Fuentes detectadas para '{concept}': {srcs}")
            return srcs
    except Exception as e:
        logging.error(f"Error getting sources: {e}")
    return []

def get_file_context(concept, sources):
    context_chunks = []
    input_dir = os.environ.get("EXTRACTOR_INPUT_DIR", "data/input_files")
    done_dir = os.environ.get("EXTRACTOR_DONE_DIR", "data/input_files/done")
    
    # Optional PyMuPDF
    try:
        import fitz
        PDF_SUPPORT = True
    except ImportError:
        PDF_SUPPORT = False

    for source in sources:
        if source.startswith("file:"):
            filename = source.split("file:", 1)[1]
            
            # Buscamos el archivo
            filepath = None
            for d in [input_dir, done_dir]:
                if not d: continue
                p = os.path.join(d, filename)
                if os.path.exists(p):
                    filepath = p
                    break
                p_bak = p + ".bak"
                if os.path.exists(p_bak):
                    filepath = p_bak
                    break
            
            if filepath:
                logging.info(f"Buscando contexto en archivo: {filepath}")
                text = ""
                ext = os.path.splitext(filepath)[1].lower().replace(".bak", "")
                if ext in [".txt", ".md"]:
                    try:
                        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                            text = f.read()
                    except Exception:
                        pass
                elif ext == ".pdf" and PDF_SUPPORT:
                    try:
                        doc = fitz.open(filepath)
                        pages = [page.get_text() for page in doc]
                        doc.close()
                        text = "\n".join(pages)
                    except Exception:
                        pass
                        
                if text:
                    concept_lower = concept.lower()
                    paragraphs = text.split("\n\n")
                    
                    # Búsqueda tolerante a saltos de línea
                    for p in paragraphs:
                        p_norm = p.replace("\n", " ").lower()
                        if concept_lower in p_norm:
                            if len(p) > 2000:
                                p = p[:2000] + "..."
                            if p not in context_chunks:
                                context_chunks.append(p)
                            if len(context_chunks) >= 5:
                                break
                                
                    # FALLBACK: Si no se encontró la frase exacta, usar los primeros 3000 chars del archivo
                    # para garantizar que seguimos en MODO ARCHIVO y no alucina.
                    if not context_chunks:
                        fallback_text = text[:3000]
                        if fallback_text not in context_chunks:
                            context_chunks.append(fallback_text)
    if context_chunks:
        return "\n...\n".join(context_chunks)
    return ""

def ask_grounding(concept, context=""):
    url = f"{OLLAMA_HOST}/api/generate"
    
    if context:
        prompt_text = PROMPT_JSON_FILE.format(concept=concept, context=context)
        logging.info(f"Pidiendo datos concretos para: '{concept}' [MODO ARCHIVO]")
    else:
        prompt_text = PROMPT_JSON.format(concept=concept)
        logging.info(f"Pidiendo datos concretos para: '{concept}' [MODO SEMILLA]")
        
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt_text,
        "stream": False,
        "options": {"temperature": 0.0}
    }
    
    try:
        response = requests.post(url, json=payload, timeout=120)
        response.raise_for_status()
        full_json = response.json()
        raw_result = full_json.get("response", "").strip()
        
        if not raw_result:
            logging.error(f"Ollama devolvió un campo 'response' vacío. JSON completo: {full_json}")
            return None

        clean_result = extract_json(raw_result)
        
        # Intentar arreglar las comillas triples si el LLM las puso de todos modos
        if '"""' in clean_result:
            # Reemplazamos los saltos de línea reales por \n temporalmente
            clean_result = clean_result.replace('\n', '\\n')
            # Quitamos las comillas triples
            clean_result = clean_result.replace('"""', '"')
            
        try:
            return json.loads(clean_result)
        except json.JSONDecodeError as je:
            logging.error(f"Error parseando JSON de Ollama: {je}")
            logging.error(f"Texto que falló: {clean_result}")
            return None

    except Exception as e:
        logging.error(f"Error de conexión o respuesta inesperada de Ollama: {e}")
        return None

def main():
    logging.info("Arrancando Motor de Grounding (ABox)... Esperando 10s para estabilizacion.")
    time.sleep(10)
    
    db_path = DB_PATH
    os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else '.', exist_ok=True)
    db = pycozo.Client('sqlite', db_path, dataframe=False)
    wm = WikiManager(os.path.dirname(db_path) if os.path.dirname(db_path) else ".")
    
    setup_db(db)
    
    iteration = 0
    while iteration < MAX_ITERATIONS:
        concept = get_next_ungrounded(db)
        if not concept:
            logging.info("Grafo al 100% aterrizado. Reintentando busqueda en 10s...")
            time.sleep(10)
            continue
            
        sources = get_concept_sources(db, concept)
        context = get_file_context(concept, sources)
            
        data = ask_grounding(concept, context)
        if not data or "descripcion" not in data:
            logging.warning("Mala resolucion. Marcando como no-descriptible.")
            run_query(db, "?[concept, description] <- $data\n:put concept_metadata {concept => description}", 
                   {"data": [[concept, "Sin descripción concluyente calculada."]]})
        else:
            desc = data["descripcion"]
            # 1. Update CozoDB
            run_query(db, "?[concept, description] <- $data\n:put concept_metadata {concept => description}", 
                   {"data": [[concept, desc]]})
            
            # 2. Update Wiki
            content, meta = wm.read_page(concept)
            if content is not None:
                meta["description"] = desc
                if len(content) < 50:
                    content = desc + "\n\n" + content
                wm.write_page(concept, content, meta)
            else:
                wm.write_page(concept, desc, {"description": desc})
            
            instancias = data.get("instancias", [])
            if isinstance(instancias, list) and len(instancias) > 0:
                hechos = []
                # Herencia de fuentes: Si el concepto padre tiene fuentes de archivo, 
                # las propagamos a las instancias para que no se pierdan en el modo semilla.
                file_sources = [s for s in sources if s.startswith("file:")]
                
                for inst_raw in instancias:
                    if not isinstance(inst_raw, str): continue
                    items = [x.strip() for x in inst_raw.split(";") if x.strip()]
                    for inst in items:
                        inst_name = inst.title()
                        # Filtro de seguridad: No crear conceptos a partir de frases de error del LLM
                        if any(x in inst_name.lower() for x in ["sin información", "no hay", "desconocido", "n/a", "no se encontró"]):
                            continue
                            
                        # Marcamos como grounding_agent
                        hechos.append([inst_name, "es_instancia_de", concept, 1.0, "grounding_agent", False])
                        # Y también le asignamos las fuentes originales del archivo para mantener el hilo
                        for fs in file_sources:
                            hechos.append([inst_name, "proviene_de_contexto", concept, 1.0, fs, False])
                
                if hechos:
                    logging.info(f"Insertadas {len(hechos)} hechos de instancia/contexto para '{concept}'")
                    run_query(db, "?[entity, attribute, value, confidence, source, is_bind] <- $data\n:put eav {entity, attribute, value => confidence, source, is_bind}", 
                           {"data": hechos})
                           
        time.sleep(2)
        iteration += 1

if __name__ == "__main__":
    main()

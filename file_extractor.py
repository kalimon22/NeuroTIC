"""
file_extractor.py — Extractor de Conocimiento desde Archivos
Procesa archivos .txt y .pdf de data/input_files/ y extrae relaciones EAV
usando el LLM SOLO como intérprete del contenido (no de su memoria interna).
"""
import os
import io
import csv
import time
import glob
import logging
import requests
import pycozo
from db_utils import setup_db, run_query
from wiki_utils import WikiManager

# Optional PDF support
try:
    import fitz  # PyMuPDF
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
    logging.warning("PyMuPDF no instalado. Solo se procesarán archivos .txt")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [EXTRACTOR] - %(message)s")

OLLAMA_HOST   = os.environ.get("OLLAMA_HOST", "http://host.docker.internal:11434")
OLLAMA_MODEL  = os.environ.get("OLLAMA_MODEL", "llama3")
INPUT_DIR     = os.environ.get("EXTRACTOR_INPUT_DIR", "data/input_files")
DONE_DIR      = os.environ.get("EXTRACTOR_DONE_DIR",  "data/input_files/done")
CHUNK_SIZE    = int(os.environ.get("EXTRACTOR_CHUNK_SIZE", "2500"))  # chars per chunk
DB_PATH       = os.environ.get("DB_PATH", "data/ontology.db")

EXTRACT_PROMPT = """You are an expert ontology extractor analyzing chunks of a document named '{filename}'.
Extract relationships from the following text fragment ONLY. 
Use these relationship types: afecta_a, generado_por, compuesto_por, propuesto_por, es_un, requiere_de, es_instancia_de, regula, prohibe, permite, establece.

TEXT:
\"\"\"
{chunk}
\"\"\"

Output STRICTLY as CSV (no code fences, no extra text).
Header: Entidad,Atributo,Valor,Confianza
Rules:
- Identify meaningful entities (concepts, laws, institutions, rights, organizations) and AVOID completely generic terms like 'artículo', 'anexo', 'ley', 'presente decreto', 'apartado'.
- 'Entidad' and 'Valor' must be concise, descriptive noun phrases.
- 'Confianza' is a float 0.0-1.0.
- If nothing meaningful can be extracted, output only the header line.
"""

META_PROMPT = """You are an expert archivist and legal/documentary analyst.
Your task is to analyze the beginning of a document and extract its metadata.
Document Name: {filename}

Extract exactly these metadata relationships in strict CSV format.
Header: Entidad,Atributo,Valor,Confianza
Rules: 
- 'Entidad' MUST ALWAYS be exactly "{filename}".
- Extract these 4 attributes (use exactly these Atributo names):
  1. "fecha_publicacion" (extract the date, or 'Desconocida' if not found)
  2. "tema_principal" (a short phrase summarizing the main topic)
  3. "emisor" (who issued it, e.g., 'Consejería de x', 'Dirección General', 'Ministerio', or 'Desconocido')
  4. "es_un" (document type, e.g., 'Ley', 'Decreto', 'Resolución', 'Orden', etc.)
- Confianza is a float 0.0-1.0.
- Output STRICTLY as CSV (no code fences, no extra text).

TEXT TO ANALYZE:
\"\"\"
{text_start}
\"\"\"
"""


def read_txt(path):
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        return f.read()


def read_pdf(path):
    if not PDF_SUPPORT:
        logging.error(f"No se puede leer PDF (PyMuPDF no instalado): {path}")
        return ""
    doc = fitz.open(path)
    pages = [page.get_text() for page in doc]
    doc.close()
    return "\n".join(pages)


def chunk_text(text, size=CHUNK_SIZE):
    """Split text into overlapping chunks."""
    chunks = []
    step = int(size * 0.85)  # 15% overlap
    for i in range(0, len(text), step):
        chunks.append(text[i:i+size])
    return chunks


def ask_extract_chunk(chunk, filename):
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": EXTRACT_PROMPT.format(chunk=chunk, filename=filename),
        "stream": False,
        "options": {"temperature": 0.0}
    }
    try:
        r = requests.post(f"{OLLAMA_HOST}/api/generate", json=payload, timeout=120)
        r.raise_for_status()
        return r.json().get("response", "")
    except Exception as ex:
        logging.error(f"Error llamando a LLM: {ex}")
        return ""

def ask_ollama(prompt):
    payload = {"model": OLLAMA_MODEL, "prompt": prompt, "stream": False, "options": {"temperature": 0.2}}
    try:
        r = requests.post(f"{OLLAMA_HOST}/api/generate", json=payload, timeout=120)
        r.raise_for_status()
        return r.json().get("response", "")
    except Exception as e:
        logging.error(f"Error en ask_ollama: {e}")
        return "Error en resumen."


def ask_extract_meta(text_start, filename):
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": META_PROMPT.format(text_start=text_start, filename=filename),
        "stream": False,
        "options": {"temperature": 0.0}
    }
    try:
        r = requests.post(f"{OLLAMA_HOST}/api/generate", json=payload, timeout=120)
        r.raise_for_status()
        return r.json().get("response", "")
    except Exception as ex:
        logging.error(f"Error llamando a Ollama para metadatos: {ex}")
        return ""


def parse_csv_to_facts(raw_csv, source_tag):
    facts = []
    try:
        f = io.StringIO(raw_csv.strip())
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue
            row = [x.strip() for x in row]
            if i == 0 and "Entidad" in row[0]:
                continue
            if len(row) >= 4:
                entity, attr, value, conf_str = row[0], row[1], row[2], row[3]
                try:
                    conf = float(conf_str)
                except ValueError:
                    conf = 0.75
                if entity and attr and value:
                    facts.append([entity.title(), attr.lower(), value.title(), conf, source_tag, False])
    except Exception as ex:
        logging.error(f"Error parseando CSV: {ex}")
    return facts


def insert_facts(db, facts):
    if not facts:
        return
    try:
        db.run(
            "?[entity, attribute, value, confidence, source, is_bind] <- $data\n"
            ":put eav {entity, attribute, value => confidence, source, is_bind}",
            {"data": facts}
        )
        logging.info(f"  → {len(facts)} hechos insertados.")
    except Exception as ex:
        logging.error(f"Error insertando hechos: {ex}")


def move_to_done(path):
    os.makedirs(DONE_DIR, exist_ok=True)
    basename = os.path.basename(path)
    dest = os.path.join(DONE_DIR, basename)
    # Avoid collision
    if os.path.exists(dest):
        dest = dest + ".bak"
    os.rename(path, dest)
    logging.info(f"Archivo procesado movido a: {dest}")


def process_file(db, path, wm):
    ext = os.path.splitext(path)[1].lower()
    logging.info(f"Procesando archivo: {path}")

    if ext in [".txt", ".md"]:
        text = read_txt(path)
    elif ext == ".pdf":
        text = read_pdf(path)
    else:
        logging.warning(f"Formato no soportado: {ext}")
        return

    if not text.strip():
        logging.warning("Archivo vacío o sin texto extraíble.")
        move_to_done(path)
        return

    source_tag = f"file:{os.path.basename(path)}"
    filename = os.path.basename(path)
    total_facts = 0

    # 1. Extract Metadata from first 4000 characters
    logging.info("  Extrayendo metadatos del documento (cabecera)...")
    head_text = text[:4000]
    meta_raw = ask_extract_meta(head_text, filename)
    if meta_raw:
        meta_facts = parse_csv_to_facts(meta_raw, source_tag)
        insert_facts(db, meta_facts)
        total_facts += len(meta_facts)

    chunks = chunk_text(text)
    logging.info(f"  {len(chunks)} fragmentos para procesar.")

    for i, chunk in enumerate(chunks):
        logging.info(f"  Procesando fragmento {i+1}/{len(chunks)}...")
        raw = ask_extract_chunk(chunk, filename)
        if raw:
            facts = parse_csv_to_facts(raw, source_tag)
            insert_facts(db, facts)
            total_facts += len(facts)
        time.sleep(1)  # Pausa entre llamadas

    logging.info(f"Archivo completado. Total hechos extraídos: {total_facts}")
    
    # 3. Create a Wiki entry for the Source (Professional summary)
    summary_prompt = f"Resume este documento legal de forma profesional para una ontología. Sé conciso pero incluye el propósito principal.\nTexto:\n{text[:2000]}"
    summary = ask_ollama(summary_prompt)
    wm.write_page(filename.title(), f"# {filename.title()}\n\n{summary}\n\nTotal hechos extraídos: {total_facts}", {"tipo": "fuente", "relaciones": total_facts})
    
    move_to_done(path)


def main():
    logging.info("Iniciando Extractor de Archivos NeuroTIC...")
    os.makedirs(INPUT_DIR, exist_ok=True)
    db = pycozo.Client('sqlite', DB_PATH, dataframe=False)
    wm = WikiManager(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else ".")

    # Initialize base tables if they don't exist
    setup_db(db)

    logging.info(f"Escaneando {INPUT_DIR} cada 10s en busca de archivos...")
    while True:
        patterns = [
            os.path.join(INPUT_DIR, "*.txt"),
            os.path.join(INPUT_DIR, "*.md"),
            os.path.join(INPUT_DIR, "*.pdf"),
        ]
        files = []
        for p in patterns:
            files.extend(glob.glob(p))

        if files:
            for filepath in files:
                # Check if already processed (exists in eav for this file)
                filename = os.path.basename(filepath)
                res = run_query(db, f"?[c] := *eav[c, _, _, _, 'file:{filename}', _] :limit 1")
                if res.get('rows'):
                    logging.info(f"  Archivo {filename} ya procesado. Saltando.")
                    move_to_done(filepath)
                    continue
                    
                process_file(db, filepath, wm)
        else:
            logging.info(f"Sin archivos pendientes en {INPUT_DIR}. Esperando...")

        time.sleep(10)


if __name__ == "__main__":
    main()

import os
import time
import requests
import pycozo
import json
import logging
import re

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [GROUNDING] - %(message)s")

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://host.docker.internal:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3")
MAX_ITERATIONS = int(os.environ.get("MAX_ITERATIONS", "1000")) # Alto, porque escanea todo
DB_PATH = os.environ.get("DB_PATH", "data/ontology.db")

PROMPT_JSON = """Eres un experto en ontologías y conocimiento general.
Se te proporciona un 'Concepto'. Tu DEBER es proveer:
1. Una descripción académica objetiva, con hechos tangibles (Max 2-3 frases).
2. Si el concepto abarca ejemplos concretos o tipos, listalos (Max 5).
   IMPORTANTE: Cada ejemplo debe ser un elemento separado en la lista. NUNCA los unas con ";" o comas en un solo string.

Concepto actual: "{concept}"

RESPONDE EXCLUSIVAMENTE CON UN OBJETO JSON VÁLIDO.
Esquema obligatorio:
{{
   "descripcion": "<texto>",
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
        res = db.run(query)
        if 'rows' in res and len(res['rows']) > 0:
            return res['rows'][0][0]
    except Exception as e:
        pass
    return None

def ask_grounding(concept):
    url = f"{OLLAMA_HOST}/api/generate"
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": PROMPT_JSON.format(concept=concept),
        "stream": False,
        # "format": "json",  <-- Lo quitamos para ganar robustez, confiamos en regex
        "options": {"temperature": 0.0}
    }
    logging.info(f"Pidiendo datos concretos para: '{concept}'")
    try:
        response = requests.post(url, json=payload, timeout=90)
        response.raise_for_status()
        full_json = response.json()
        raw_result = full_json.get("response", "").strip()
        
        if not raw_result:
            logging.error(f"Ollama devolvió un campo 'response' vacío. JSON completo: {full_json}")
            return None

        # Tentativa de limpieza de JSON
        clean_result = extract_json(raw_result)
        
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
    
    while True:
        try:
            db.run("?[c] := *concept_metadata[c, _]:limit 1")
            break
        except:
            logging.info("Esperando a que main.py cree las tablas...")
            time.sleep(5)
            
    iteration = 0
    while iteration < MAX_ITERATIONS:
        concept = get_next_ungrounded(db)
        if not concept:
            logging.info("Grafo al 100% aterrizado. Reintentando busqueda en 10s...")
            time.sleep(10)
            continue
            
        data = ask_grounding(concept)
        if not data or "descripcion" not in data:
            logging.warning("Mala resolucion. Marcando como no-descriptible.")
            # Parameterized to avoid exact string quoting issues
            db.run("?[concept, description] <- $data\n:put concept_metadata {concept => description}", 
                   {"data": [[concept, "Sin descripción concluyente calculada."]]})
        else:
            desc = data["descripcion"]
            db.run("?[concept, description] <- $data\n:put concept_metadata {concept => description}", 
                   {"data": [[concept, desc]]})
            
            instancias = data.get("instancias", [])
            if isinstance(instancias, list) and len(instancias) > 0:
                hechos = []
                for inst_raw in instancias:
                    if not isinstance(inst_raw, str): continue
                    # Split por si acaso el modelo desobedece y concatena
                    items = [x.strip() for x in inst_raw.split(";") if x.strip()]
                    for inst in items:
                        hechos.append([inst.title(), "es_instancia_de", concept, 1.0, "grounding_agent", False])
                
                if hechos:
                    logging.info(f"Insertadas {len(hechos)} instancias concretas para '{concept}'")
                    db.run("?[entity, attribute, value, confidence, source, is_bind] <- $data\n:put eav {entity, attribute, value => confidence, source, is_bind}", 
                           {"data": hechos})
                           
        time.sleep(2)
        iteration += 1

if __name__ == "__main__":
    main()

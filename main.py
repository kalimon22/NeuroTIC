import os
import time
import requests
import pycozo
import csv
import io
import collections
import logging
from db_utils import run_query, setup_db
from wiki_utils import WikiManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://host.docker.internal:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3")
SEED_CONCEPT = os.environ.get("SEED_CONCEPT", "Gravedad")
MAX_ITERATIONS = int(os.environ.get("MAX_ITERATIONS", "100"))
DB_PATH = os.environ.get("DB_PATH", "data/ontology.db")
BASE_DIR = os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else "."

PROMPT_TEMPLATE = """Eres un Motor Autónomo de Ontología Física y Gestor de Wiki. 
Tu tarea es analizar el concepto: "{concept}"

Debes producir una página de Wiki en formato Markdown que incluya:
1. YAML Frontmatter con los campos:
   - description: Una breve definición (1-2 frases).
   - relations: Una lista de objetos {{attr: "relacion", target: "Concepto"}}
     Usa verbos/atributos: afecta_a, generado_por, compuesto_por, propuesto_por, es_un, requiere_de.
2. Un cuerpo de texto con una explicación académica detallada pero concisa (2-3 párrafos).
3. Usa [[WikiLinks]] para otros conceptos científicos mencionados.

RESPONDE ÚNICAMENTE CON EL CONTENIDO DEL ARCHIVO MARKDOWN (sin bloques de código extra ni introducciones).
"""



def ask_ollama(concept):
    url = f"{OLLAMA_HOST}/api/generate"
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": PROMPT_TEMPLATE.format(concept=concept),
        "stream": False,
        "options": {
            "temperature": 0.1 # Muy bajo para respuestas mas deterministas y menos chat
        }
    }
    logging.info(f"[OLLAMA] Pidiendo extracción relacional para: '{concept}'")
    try:
        # Request con un timemout alto ya que LLMs locales pueden demorar
        response = requests.post(url, json=payload, timeout=120)
        response.raise_for_status()
        result = response.json().get("response", "")
        return result
    except requests.exceptions.RequestException as e:
        logging.error(f"Error conectando a Ollama: {e}")
        return ""

def parse_wiki_output(raw_text, expected_entity):
    """
    Parses the Markdown/YAML output from the LLM.
    Returns: (metadata, content, facts_for_db)
    """
    metadata = {"description": "", "relations": []}
    content = raw_text
    facts = []

    # 1. Parse YAML if present
    if raw_text.strip().startswith("---"):
        parts = raw_text.split("---", 2)
        if len(parts) >= 3:
            yml_text = parts[1].strip()
            content = parts[2].strip()
            # Basic manual parse since we might not have PyYAML in docker
            try:
                import yaml
                data = yaml.safe_load(yml_text) or {}
                metadata.update(data)
            except Exception:
                # Naive line parser
                for line in yml_text.split("\n"):
                    if ":" in line:
                        k, v = line.split(":", 1)
                        metadata[k.strip()] = v.strip().strip('"').strip("'")

    # 2. Convert metadata['relations'] to DB facts
    # Format might be list of dicts or just text lines
    rels = metadata.get("relations", [])
    if isinstance(rels, list):
        for r in rels:
            if isinstance(r, dict):
                attr = r.get("attr", "relacionado_con")
                target = r.get("target", "Desconocido")
                facts.append([expected_entity.title(), attr.lower(), target.title(), 1.0, "wiki_engine", False])
    
    # 3. Handle descriptions
    if metadata.get("description"):
        # We can also return this to update concept_metadata table
        pass

    return metadata, content, facts

def apply_binds(db):
    logging.info("[BINDS] Evaluando lógicas Datalog...")
    # Ejemplo de Bind Transitivo: Si A afecta a B y B afecta a C -> A afecta indirectamente a C
    # Las reglas de Datalog evitan bucles infinitos si evaluamos con un límite, pero en put/insert,
    # re-evaluara todas las veces que haya nuevos en vez de hacerlo incremental de primeras 
    # de un modo simple inserta/sobrescribe usando la clave primaria.
    bind_transitivo = """
    ?[entity, attribute, value, confidence, source, is_bind] := 
        *eav[A, act1, B, c1, _, _], 
        *eav[B, act2, C, c2, _, _],
        act1 == "afecta_a", act2 == "afecta_a",
        entity = A, value = C, A != C,
        attribute = "afecta_indirectamente_a",
        confidence = c1 * c2,
        source = "bind_transitivo",
        is_bind = true
    :put eav {entity, attribute, value => confidence, source, is_bind}
    """
    try:
        res = run_query(db, bind_transitivo)
        nuevos_binds = res.get("display") if isinstance(res, dict) else str(res)
        logging.info(f"[BINDS] Transitivo OK. (Registros afectados según el motor o ya existentes)")
    except Exception as e:
        logging.error(f"[BINDS] Error en bind transitivo: {e}")

import collections

def get_next_orphan(db, seed):
    """
    Finds the next orphan concept using a Breadth-First Search (BFS) from the seed.
    This ensures we explore the 'neighborhood' of the seed before going deeper.
    """
    try:
        # 1. Get all relations to build adjacency list
        res = run_query(db, "?[s, v] := *eav[s, _, v, _, _, _]")
        facts = res.get('rows', [])
        
        # 2. Get already expanded concepts (those that have been subjects of 'wiki_engine' or 'ollama' facts)
        res_exp = run_query(db, "?[e] := *eav[e, _, _, _, 'wiki_engine', false]")
        expanded = {r[0] for r in res_exp.get('rows', [])}
        
        # Also include 'ollama' legacy if present
        res_exp_legacy = run_query(db, "?[e] := *eav[e, _, _, _, 'ollama', false]")
        expanded.update({r[0] for r in res_exp_legacy.get('rows', [])})
        
        # 3. Build adjacency list
        adj = collections.defaultdict(list)
        for s, v in facts:
            adj[s].append(v)
            
        # 4. BFS search starting from the seed
        queue = collections.deque([seed] if seed else [])
        visited = {seed} if seed else set()
        
        # If seed itself is not expanded, start there
        if seed and seed not in expanded:
            return seed

        while queue:
            node = queue.popleft()
            if node and node not in expanded:
                return node
            for neighbor in adj[node]:
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)

        # 5. GLOBAL FALLBACK: If BFS found nothing, find ANY concept not in 'expanded'
        # Useful for BOCYL files that are not connected to the seed.
        res_all = run_query(db, "?[e] := *eav[e, _, _, _, _, _] :limit 100")
        all_concepts = {r[0] for r in res_all.get('rows', [])}
        for c in all_concepts:
            if c not in expanded:
                logging.info(f"[ENGINE] Salto global detectado hacia: {c}")
                return c

        return None
    except Exception as ex:
        logging.error(f"Error en get_next_orphan: {ex}")
        return None

def m_info(db):
    try:
         total = run_query(db, "?[count] := *eav[e,a,v,c,s,b], count=count(e,a,v)")
         logging.info(f"==> ESTADO ACTUAL: Hechos totales en EAV = {total}")
    except:
         pass

def main():
    logging.info("Iniciando Motor Autonomo de Ontologia Fisica...")
    
    db_path = DB_PATH
    logging.info(f"Abriendo db embebida en: {db_path}")
    os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else '.', exist_ok=True)
    db = pycozo.Client('sqlite', db_path, dataframe=False)
    wm = WikiManager(BASE_DIR)

    setup_db(db)
    
    # Comprobar si hay conceptos por expandir
    current_concept = get_next_orphan(db, SEED_CONCEPT)
    if not current_concept and SEED_CONCEPT:
        logging.info(f"No hay huerfanos. Iniciando con semilla: {SEED_CONCEPT}")
        current_concept = SEED_CONCEPT
    
    iteration = 0
    while iteration < MAX_ITERATIONS:
        if not current_concept:
            logging.info("Esperando nuevos terminos/huerfanos derivados de archivos... (10s)")
            time.sleep(10)
            current_concept = get_next_orphan(db, SEED_CONCEPT)
            if not current_concept and SEED_CONCEPT:
                current_concept = SEED_CONCEPT
            continue
            
        logging.info(f"--- Iteracion {iteration+1} | Concentrandose en: {current_concept} ---")
        
        # 1. Extraer LLM (Wiki + Relations)
        respuesta = ask_ollama(current_concept)
        if not respuesta:
            logging.warning("El LLM falló al intentar explicar.")
            current_concept = get_next_orphan(db, SEED_CONCEPT)
            iteration += 1
            continue

        # 2. Parsear Output
        meta, content, hechos = parse_wiki_output(respuesta, current_concept)
        
        # 3. Guardar en Wiki
        wm.write_page(current_concept, content, meta)
        logging.info(f"Página de Wiki guardada para: {current_concept}")

        # 4. Sincronizar con CozoDB
        if hechos:
            logging.info(f"Sincronizando {len(hechos)} relaciones al grafo...")
            put_query = "?[entity, attribute, value, confidence, source, is_bind] <- $data\n:put eav {entity, attribute, value => confidence, source, is_bind}"
            run_query(db, put_query, {"data": hechos})

        # 5. Guardar Metadatos en CozoDB (para retrocompatibilidad con explorer)
        if meta.get("description"):
            put_meta = "?[concept, description] <- $data\n:put concept_metadata {concept => description}"
            run_query(db, put_meta, {"data": [[current_concept, meta["description"]]]})

        # 6. Magia Relacional: Logica Datalog (Binds)
        apply_binds(db)

        # 7. Elegir el siguiente huerfano
        siguiente = get_next_orphan(db, SEED_CONCEPT)
        if siguiente == current_concept: # Evitar recursiones atrapadas si algo fallo en el put
             logging.warning("Mismo orfanato re-solicitado. Posible error Datalog.")
             break
        current_concept = siguiente
        iteration += 1
        
        # Una pequena pausa para no saturar Ollama/logistica
        time.sleep(2)

    logging.info("Motor terminado por llegar al límite de iteraciones o agotar el grafo.")

if __name__ == "__main__":
    main()

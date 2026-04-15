import os
import time
import requests
import pycozo
import csv
import io
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://host.docker.internal:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3")
SEED_CONCEPT = os.environ.get("SEED_CONCEPT", "Gravedad")
MAX_ITERATIONS = int(os.environ.get("MAX_ITERATIONS", "100"))
DB_PATH = os.environ.get("DB_PATH", "data/ontology.db")

PROMPT_TEMPLATE = """Eres un Motor Autónomo de Ontología Física. Nos enfocamos en construir un modelo del mundo lógico-relacional de la física (física clásica, cuántica, cosmología).
El concepto a analizar es: "{concept}"

Extrae entre 3 y 6 relaciones de primera línea para este concepto. Utiliza verbos/atributos unificados preferiblemente como: afecta_a, generado_por, compuesto_por, propuesto_por, es_un, requiere_de.
RESPONDE ESTRICTAMENTE EN FORMATO CSV PURO (sin recuadros ```csv ni texto extra).
La cabecera debe ser: Entidad,Atributo,Valor,Confianza
Todos los valores deben ir separados por comas y sin comillas extrañas. 'Entidad' siempre debe ser exactamente el concepto analizado en esta llamada. 'Confianza' debe ser numérico entre 0.0 y 1.0.
"""

def setup_db(db):
    logging.info("Inicializando esquemas relacionales en CozoDB...")
    try:
        # primary key: entity, attribute, value. No key attributes: confidence, source, is_bind
        db.run(":create eav {entity: String, attribute: String, value: String => confidence: Float, source: String, is_bind: Bool}")
    except Exception as e:
        if "conflicts with an existing one" not in str(e):
             logging.error(f"Error eav: {e}")
             
    try:
        db.run(":create concept_metadata {concept: String => description: String}")
    except Exception as e:
        if "conflicts with an existing one" not in str(e):
             logging.error(f"Error concept_metadata: {e}")
             
    logging.info("Estructura de Base de Datos verificada.")

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

def parse_llm_csv(raw_csv_text, expected_entity):
    datos = []
    # Usamos io.StringIO y csv reader para manejar las lineas de forma robusta
    f = io.StringIO(raw_csv_text.strip())
    reader = csv.reader(f)
    for i, _row in enumerate(reader):
        if not _row: continue
        row = [x.strip() for x in _row]
        if i == 0 and "Entidad" in row[0]:
            continue # Skip header
        if len(row) >= 4:
            e, a, v, c_str = row[0], row[1], row[2], row[3]
            if e.lower() != expected_entity.lower():
                # Forzamos que la entidad sea la esperada
                e = expected_entity

            try:
                c = float(c_str)
            except ValueError:
                c = 0.8
            
            # Limpieza: Si el valor tiene semicolones, el modelo ignoró las instrucciones y los unió.
            # Los separamos en múltiples hechos.
            values = [val.strip() for val in v.split(";") if val.strip()]
            for val in values:
                datos.append([e.title(), a.lower(), val.title(), c, "ollama", False])
    return datos

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
        res = db.run(bind_transitivo)
        nuevos_binds = res.get("display") if isinstance(res, dict) else str(res)
        logging.info(f"[BINDS] Transitivo OK. (Registros afectados según el motor o ya existentes)")
    except Exception as e:
        logging.error(f"[BINDS] Error en bind transitivo: {e}")

def get_next_orphan(db):
    consulta = """
    # Encontramos valores que JAMAS han sido mapeados como Entidades de primer nivel
    # para ser analizadas (source="ollama", is_bind=false)
    # y los elegimos
    hecho_entidad[e] := *eav[e, _, _, _, src, no_bind], src="ollama", no_bind=false
    ?[huerfano] := *eav[_, _, huerfano, _, _, _], not hecho_entidad[huerfano]
    :limit 1
    """
    try:
        res = db.run(consulta)
        if isinstance(res, dict) and 'rows' in res and len(res['rows']) > 0:
            return res['rows'][0][0]
    except Exception as e:
        pass
    return None

def m_info(db):
    try:
         total = db.run("?[count] := *eav[e,a,v,c,s,b], count=count(e,a,v)")
         logging.info(f"==> ESTADO ACTUAL: Hechos totales en EAV = {total}")
    except:
         pass

def main():
    logging.info("Iniciando Motor Autonomo de Ontologia Fisica...")
    
    db_path = DB_PATH
    logging.info(f"Abriendo db embebida en: {db_path}")
    os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else '.', exist_ok=True)
    db = pycozo.Client('sqlite', db_path, dataframe=False)

    setup_db(db)
    
    # Comprobar si hay conceptos por expandir
    current_concept = get_next_orphan(db)
    if not current_concept:
        logging.info(f"No hay huerfanos. Iniciando con semilla: {SEED_CONCEPT}")
        current_concept = SEED_CONCEPT
        # Le añadimos un hecho pivot para no estar vacio, o sencillamente que tire.
        # No hace falta insertarlo aqui, el llm lo insertara como entidad
    
    iteration = 0
    while iteration < MAX_ITERATIONS:
        if not current_concept:
            logging.info("El Grafo se ha quedado sin nuevos terminos/huerfanos para expandir. ¡Exito ontolologico!")
            break
            
        logging.info(f"--- Iteracion {iteration+1} | Concentrandose en: {current_concept} ---")
        
        # 1. Extraer LLM
        respuesta = ask_ollama(current_concept)
        if not respuesta:
            logging.warning("El LLM falló al intentar explicar. Tratando otro huérfano si existe.")
            # Un atajo: insertamos un EAV para 'quemar' este concepto y que no nos atranque el bucle
            burn = f"?[entity, attribute, value, confidence, source, is_bind] <- [['{current_concept}', 'es_ininteligible', 'Vacio', 0.0, 'sistema', false]]\n:put eav {{entity, attribute, value => confidence, source, is_bind}}"
            db.run(burn)
            current_concept = get_next_orphan(db)
            iteration += 1
            continue

        # 2. Parsear CSV
        hechos = parse_llm_csv(respuesta, current_concept)
        if not hechos:
            logging.warning("No se pudieron parsear hechos desde el csv de Ollama. Respuesta recibida:")
            logging.warning(respuesta)
            burn = f"?[entity, attribute, value, confidence, source, is_bind] <- [['{current_concept}', 'genero_respuesta_no_csv', 'Error', 0.0, 'sistema', false]]\n:put eav {{entity, attribute, value => confidence, source, is_bind}}"
            db.run(burn)
        else:
            # 3. Insertar a CozoDB
            logging.info(f"Insertando {len(hechos)} hechos al grafo...")
            put_query = "?[entity, attribute, value, confidence, source, is_bind] <- $data\n:put eav {entity, attribute, value => confidence, source, is_bind}"
            db.run(put_query, {"data": hechos})

            # 4. Magia Relacional: Logica Datalog (Binds)
            apply_binds(db)

        m_info(db)

        # 5. Elegir el siguiente huerfano para la recursión del arbol de conocimientos
        siguiente = get_next_orphan(db)
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

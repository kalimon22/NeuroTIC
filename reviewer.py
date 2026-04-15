"""
reviewer.py — Agente Revisor de la Ontología NeuroTIC
Escanea hechos en lotes, los envía al LLM para revisión y aplica correcciones
- En modo AUTO (REVIEW_MODE=auto): aplica los cambios directamente
- En modo PENDING (REVIEW_MODE=pending): los almacena en `pending_review` para revisión humana
"""
import os
import time
import json
import uuid
import logging
import requests
import pycozo

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [REVIEWER] - %(message)s")

OLLAMA_HOST  = os.environ.get("OLLAMA_HOST", "http://host.docker.internal:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3")
REVIEW_MODE  = os.environ.get("REVIEW_MODE", "pending")  # "auto" | "pending"
BATCH_SIZE   = int(os.environ.get("REVIEW_BATCH_SIZE", "10"))
SLEEP_SECS   = int(os.environ.get("REVIEW_SLEEP", "30"))
DB_PATH      = os.environ.get("DB_PATH", "data/ontology.db")

REVIEW_PROMPT = """You are a data quality expert for a physics ontology knowledge graph.
You will receive a list of EAV (Entity, Attribute, Value) facts from the database.
Your task: identify facts that have obvious issues such as:
- Spelling mistakes in entity or value names (e.g. "Gravvedad" should be "Gravedad")
- Attribute names inconsistent with the vocabulary: afecta_a, generado_por, compuesto_por, propuesto_por, es_un, requiere_de, es_instancia_de, afecta_indirectamente_a
- Clearly wrong relationships

For each fix, output a JSON object. Output ONLY a JSON array of fixes (empty [] if all looks fine).
Each fix must have: old_entity, old_attr, old_val, new_entity, new_attr, new_val, reason.

Facts to review:
{facts}

Respond with ONLY the JSON array, no other text."""


def setup_pending_table(db):
    try:
        db.run(":create pending_review {id: String => old_entity: String, old_attr: String, old_val: String, new_entity: String, new_attr: String, new_val: String, reason: String}")
        logging.info("Tabla pending_review creada.")
    except Exception as e:
        if "conflicts with an existing one" not in str(e):
            logging.error(f"Error creando pending_review: {e}")

    # reviewed_facts tracks which EAV triplets have already been reviewed
    try:
        db.run(":create reviewed_fact {entity: String, attribute: String, value: String}")
        logging.info("Tabla reviewed_fact creada.")
    except Exception as e:
        if "conflicts with an existing one" not in str(e):
            logging.error(f"Error creando reviewed_fact: {e}")


def get_unreviewed_batch(db, batch_size):
    """Return up to batch_size EAV rows that haven't been reviewed yet."""
    query = """
    reviewed[e, a, v] := *reviewed_fact[e, a, v]
    ?[entity, attribute, value, confidence, source] :=
        *eav[entity, attribute, value, confidence, source, _],
        source != "llm_reviewer",
        not reviewed[entity, attribute, value]
    :limit $n
    """
    try:
        res = db.run(query, {"n": batch_size})
        return res.get('rows', [])
    except Exception as ex:
        logging.error(f"Error obteniendo batch: {ex}")
        return []


def ask_reviewer(facts_list):
    """Call LLM asking it to review the provided facts."""
    facts_text = "\n".join(
        f"- Entity='{r[0]}', Attribute='{r[1]}', Value='{r[2]}' (confidence={r[3]:.2f}, source={r[4]})"
        for r in facts_list
    )
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": REVIEW_PROMPT.format(facts=facts_text),
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.0}
    }
    try:
        r = requests.post(f"{OLLAMA_HOST}/api/generate", json=payload, timeout=180)
        r.raise_for_status()
        raw = r.json().get("response", "[]")
        return json.loads(raw)
    except Exception as ex:
        logging.error(f"Error llamando al LLM revisor: {ex}")
        return []


def mark_reviewed(db, facts_list):
    """Mark facts as reviewed so we don't process them again."""
    data = [[r[0], r[1], r[2]] for r in facts_list]
    try:
        db.run("?[entity, attribute, value] <- $data\n:put reviewed_fact {entity, attribute, value}", {"data": data})
    except Exception as ex:
        logging.error(f"Error marcando como revisados: {ex}")


def apply_fix_auto(db, fix):
    """Apply a fix directly to the eav table."""
    try:
        db.run("?[entity, attribute, value] <- $data\n:rm eav {entity, attribute, value}",
               {"data": [[fix['old_entity'], fix['old_attr'], fix['old_val']]]})
        db.run("?[entity, attribute, value, confidence, source, is_bind] <- $data\n:put eav {entity, attribute, value => confidence, source, is_bind}",
               {"data": [[fix['new_entity'], fix['new_attr'], fix['new_val'], 1.0, 'llm_reviewer', False]]})
        logging.info(f"[AUTO] Corregido: [{fix['old_entity']}, {fix['old_attr']}, {fix['old_val']}] → [{fix['new_entity']}, {fix['new_attr']}, {fix['new_val']}]")
    except Exception as ex:
        logging.error(f"Error aplicando fix auto: {ex}")


def store_fix_pending(db, fix):
    """Store a fix in pending_review for human approval."""
    review_id = str(uuid.uuid4())
    try:
        db.run(
            "?[id, old_entity, old_attr, old_val, new_entity, new_attr, new_val, reason] <- $data\n"
            ":put pending_review {id => old_entity, old_attr, old_val, new_entity, new_attr, new_val, reason}",
            {"data": [[
                review_id,
                fix['old_entity'], fix['old_attr'], fix['old_val'],
                fix['new_entity'], fix['new_attr'], fix['new_val'],
                fix.get('reason', '')
            ]]}
        )
        logging.info(f"[PENDING] Revisión encolada ({review_id[:8]}): {fix['old_entity']} → {fix['new_entity']}")
    except Exception as ex:
        logging.error(f"Error guardando fix pendiente: {ex}")


def main():
    logging.info(f"Iniciando Motor de Revisión. Modo={REVIEW_MODE}, Batch={BATCH_SIZE}")
    time.sleep(15)  # Esperar a que el resto arranque

    db_path = DB_PATH
    os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else '.', exist_ok=True)
    db = pycozo.Client('sqlite', db_path, dataframe=False)

    # Wait for main.py to create the base tables
    while True:
        try:
            db.run("?[c] := *eav[c, _, _, _, _, _] :limit 1")
            break
        except Exception:
            logging.info("Esperando a que se creen las tablas base...")
            time.sleep(5)

    setup_pending_table(db)

    while True:
        batch = get_unreviewed_batch(db, BATCH_SIZE)
        if not batch:
            logging.info(f"Sin hechos sin revisar. Esperando {SLEEP_SECS}s...")
            time.sleep(SLEEP_SECS)
            continue

        logging.info(f"Revisando lote de {len(batch)} hechos...")
        fixes = ask_reviewer(batch)

        valid_fixes = [
            f for f in fixes
            if isinstance(f, dict) and all(k in f for k in ('old_entity', 'old_attr', 'old_val', 'new_entity', 'new_attr', 'new_val'))
        ]
        logging.info(f"El LLM propone {len(valid_fixes)} correcciones.")

        for fix in valid_fixes:
            if REVIEW_MODE == "auto":
                apply_fix_auto(db, fix)
            else:
                store_fix_pending(db, fix)

        mark_reviewed(db, batch)
        time.sleep(SLEEP_SECS)


if __name__ == "__main__":
    main()

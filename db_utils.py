import time
import random
import logging

def run_query(db, script, params=None, max_retries=10):
    """
    Executes a CozoDB script with a retry mechanism for 'database is locked' errors.
    Uses exponential backoff with jitter to reduce contention.
    """
    last_ex = None
    for i in range(max_retries):
        try:
            return db.run(script, params)
        except Exception as e:
            err_msg = str(e).lower()
            if "database is locked" in err_msg or "(code 5)" in err_msg:
                last_ex = e
                # Exponential backoff: 0.1s, 0.2s, 0.4s, 0.8s... plus random jitter
                wait_time = (0.1 * (2 ** i)) + (random.random() * 0.1)
                logging.warning(f"[DB] Database locked, retrying in {wait_time:.2f}s... (Attempt {i+1}/{max_retries})")
                time.sleep(wait_time)
                continue
            # If it's a different error, raise it immediately
            raise e
    
    logging.error(f"[DB] Failed to execute query after {max_retries} retries due to locking.")
    raise last_ex

def setup_db(db):
    logging.info("Inicializando esquemas relacionales en CozoDB...")
    try:
        # primary key: entity, attribute, value. No key attributes: confidence, source, is_bind
        run_query(db, ":create eav {entity: String, attribute: String, value: String => confidence: Float, source: String, is_bind: Bool}")
    except Exception as e:
        if "conflicts with an existing one" not in str(e):
             logging.error(f"Error eav: {e}")
             
    try:
        run_query(db, ":create concept_metadata {concept: String => description: String}")
    except Exception as e:
        if "conflicts with an existing one" not in str(e):
             logging.error(f"Error concept_metadata: {e}")
             
    logging.info("Estructura de Base de Datos verificada.")

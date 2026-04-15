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
CHUNK_SIZE    = int(os.environ.get("EXTRACTOR_CHUNK_SIZE", "1500"))  # chars per chunk
DB_PATH       = "data/ontology.db"

EXTRACT_PROMPT = """You are an information extraction assistant.
Extract relationships from the following text fragment ONLY. Do NOT use any external knowledge.
Use these relationship types: afecta_a, generado_por, compuesto_por, propuesto_por, es_un, requiere_de, es_instancia_de.

TEXT:
\"\"\"
{chunk}
\"\"\"

Output STRICTLY as CSV (no code fences, no extra text).
Header: Entidad,Atributo,Valor,Confianza
Rules:
- Entidad and Valor must be noun phrases found in the text.
- Confianza is a float 0.0-1.0 based on how certain you are.
- If nothing can be extracted, output only the header line.
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


def ask_extract(chunk):
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": EXTRACT_PROMPT.format(chunk=chunk),
        "stream": False,
        "options": {"temperature": 0.0}
    }
    try:
        r = requests.post(f"{OLLAMA_HOST}/api/generate", json=payload, timeout=120)
        r.raise_for_status()
        return r.json().get("response", "")
    except Exception as ex:
        logging.error(f"Error llamando a Ollama: {ex}")
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


def process_file(db, path):
    ext = os.path.splitext(path)[1].lower()
    logging.info(f"Procesando archivo: {path}")

    if ext == ".txt":
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
    chunks = chunk_text(text)
    logging.info(f"  {len(chunks)} fragmentos para procesar.")

    total_facts = 0
    for i, chunk in enumerate(chunks):
        logging.info(f"  Procesando fragmento {i+1}/{len(chunks)}...")
        raw = ask_extract(chunk)
        if raw:
            facts = parse_csv_to_facts(raw, source_tag)
            insert_facts(db, facts)
            total_facts += len(facts)
        time.sleep(1)  # Pausa entre llamadas

    logging.info(f"Archivo completado. Total hechos extraídos: {total_facts}")
    move_to_done(path)


def main():
    logging.info("Iniciando Extractor de Archivos NeuroTIC...")
    os.makedirs(INPUT_DIR, exist_ok=True)

    db = pycozo.Client('sqlite', DB_PATH, dataframe=False)

    # Wait for base tables
    while True:
        try:
            db.run("?[c] := *eav[c, _, _, _, _, _] :limit 1")
            break
        except Exception:
            logging.info("Esperando tablas base...")
            time.sleep(5)

    logging.info(f"Escaneando {INPUT_DIR} cada 10s en busca de archivos...")
    while True:
        patterns = [
            os.path.join(INPUT_DIR, "*.txt"),
            os.path.join(INPUT_DIR, "*.pdf"),
        ]
        files = []
        for p in patterns:
            files.extend(glob.glob(p))

        if files:
            for filepath in files:
                process_file(db, filepath)
        else:
            logging.info(f"Sin archivos pendientes en {INPUT_DIR}. Esperando...")

        time.sleep(10)


if __name__ == "__main__":
    main()

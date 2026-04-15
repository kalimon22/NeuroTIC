"""
viewer.py — NeuroTIC Web Explorer + API (Multi-Ontología)
Gestiona múltiples ontologías aisladas y sus workers desde la web.
"""
import os
import sys
import json
import signal
import subprocess
import collections
import requests
import logging
from datetime import datetime
from flask import Flask, jsonify, render_template, request

import pycozo
from ontology_manager import (
    load_registry, list_ontologies, get_ontology,
    create_ontology, delete_ontology
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [VIEWER] - %(message)s")

app = Flask(__name__)

OLLAMA_HOST  = os.environ.get("OLLAMA_HOST", "http://host.docker.internal:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3")

# In-memory worker process registry:  {onto_name: {worker_name: Popen}}
running_workers: dict = {}

# Worker definitions
WORKER_SCRIPTS = {
    "ontology-engine":  "main.py",
    "grounding-engine": "grounding.py",
    "reviewer":         "reviewer.py",
    "file-extractor":   "file_extractor.py",
}

# ── DB helpers ────────────────────────────────────────────────

def get_db(onto_name: str | None = None):
    """Return a CozoDB client for the given ontology name.
    Falls back to the first available ontology, or the legacy path."""
    if onto_name:
        entry = get_ontology(onto_name)
        if entry:
            db_path = entry["db_path"]
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            return pycozo.Client('sqlite', db_path, dataframe=False)
    # Legacy / fallback
    fallback = "data/ontology.db"
    return pycozo.Client('sqlite', fallback, dataframe=False)


def onto_param():
    """Extract ?onto= from the current request."""
    return request.args.get("onto") or request.json.get("onto") if request.is_json else request.args.get("onto")


# ── STATIC / INDEX ────────────────────────────────────────────

@app.route('/')
def home():
    return render_template('index.html')


# ── ONTOLOGY CRUD ─────────────────────────────────────────────

@app.route('/api/ontologies', methods=['GET'])
def api_list_ontologies():
    ontos = list_ontologies()
    result = []
    for o in ontos:
        # Count facts if DB exists
        facts = 0
        if os.path.exists(o["db_path"]):
            try:
                db = pycozo.Client('sqlite', o["db_path"], dataframe=False)
                res = db.run("?[n] := *eav[e,a,v,_,_,_], n = count(e,a,v)")
                rows = res.get('rows', [])
                facts = rows[0][0] if rows else 0
            except Exception:
                facts = 0

        workers_status = {}
        for wname in WORKER_SCRIPTS:
            proc = running_workers.get(o["name"], {}).get(wname)
            if proc is not None:
                workers_status[wname] = "running" if proc.poll() is None else "stopped"
            else:
                workers_status[wname] = "stopped"

        result.append({**o, "facts": facts, "workers": workers_status})
    return jsonify(result)


@app.route('/api/ontologies', methods=['POST'])
def api_create_ontology():
    data = request.json or {}
    name = data.get("name", "").strip()
    mode = data.get("mode", "seed")
    seed = data.get("seed", "").strip()
    description = data.get("description", "").strip()

    if not name:
        return jsonify({"status": "error", "msg": "El nombre es obligatorio."})
    if mode not in ("seed", "files"):
        return jsonify({"status": "error", "msg": "Modo debe ser 'seed' o 'files'."})
    if mode == "seed" and not seed:
        return jsonify({"status": "error", "msg": "La semilla es obligatoria en modo seed."})

    try:
        entry = create_ontology(name, mode, seed, description)
        return jsonify({"status": "ok", "ontology": entry})
    except ValueError as e:
        return jsonify({"status": "error", "msg": str(e)})


@app.route('/api/ontologies/<name>', methods=['DELETE'])
def api_delete_ontology(name):
    # Stop all workers first
    _stop_all_workers(name)
    ok = delete_ontology(name)
    if ok:
        return jsonify({"status": "ok"})
    return jsonify({"status": "error", "msg": "Ontología no encontrada."})


# ── WORKER MANAGEMENT ─────────────────────────────────────────

def _build_worker_env(onto: dict, worker_name: str, model: str | None = None) -> dict:
    env = os.environ.copy()
    env["DB_PATH"]         = onto["db_path"]
    env["OLLAMA_HOST"]     = OLLAMA_HOST
    env["OLLAMA_MODEL"]    = model or OLLAMA_MODEL
    env["PYTHONUNBUFFERED"] = "1"
    if worker_name == "ontology-engine":
        env["SEED_CONCEPT"]    = onto.get("seed", "Gravedad")
        env["MAX_ITERATIONS"]  = "10000"
    if worker_name == "grounding-engine":
        env["MAX_ITERATIONS"]  = "100000"
    if worker_name == "reviewer":
        env["REVIEW_MODE"]     = env.get("REVIEW_MODE", "pending")
        env["REVIEW_BATCH_SIZE"] = "10"
        env["REVIEW_SLEEP"]    = "60"
    if worker_name == "file-extractor":
        env["EXTRACTOR_INPUT_DIR"] = onto["input_dir"]
        env["EXTRACTOR_DONE_DIR"]  = onto["done_dir"]
        env["EXTRACTOR_CHUNK_SIZE"] = "1500"
    return env


def _get_worker_log_path(onto_name: str, worker_name: str) -> str:
    slug = onto_name.lower().replace(" ", "_")
    log_dir = "data/logs"
    os.makedirs(log_dir, exist_ok=True)
    return f"{log_dir}/{worker_name}_{slug}.log"


def _stop_all_workers(onto_name: str):
    for wname, proc in list(running_workers.get(onto_name, {}).items()):
        if proc and proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try: proc.kill()
                except Exception: pass
    running_workers.pop(onto_name, None)


@app.route('/api/ontologies/<name>/workers', methods=['GET'])
def api_worker_status(name):
    onto = get_ontology(name)
    if not onto:
        return jsonify({"status": "error", "msg": "Ontología no encontrada."})
    result = {}
    for wname in WORKER_SCRIPTS:
        proc = running_workers.get(name, {}).get(wname)
        if proc is not None and proc.poll() is None:
            result[wname] = "running"
        else:
            result[wname] = "stopped"
    return jsonify(result)


@app.route('/api/ontologies/<name>/workers/<worker>', methods=['POST'])
def api_worker_action(name, worker):
    onto = get_ontology(name)
    if not onto:
        return jsonify({"status": "error", "msg": "Ontología no encontrada."})
    if worker not in WORKER_SCRIPTS:
        return jsonify({"status": "error", "msg": f"Worker desconocido: {worker}"})

    action = (request.json or {}).get("action", "start")
    model  = (request.json or {}).get("model")

    # Guard: ontology-engine only makes sense in seed mode
    if worker == "ontology-engine" and onto["mode"] != "seed":
        return jsonify({"status": "error", "msg": "El ontology-engine solo es válido en modo 'seed'."})
    # Guard: file-extractor only makes sense in files mode
    if worker == "file-extractor" and onto["mode"] != "files":
        return jsonify({"status": "error", "msg": "El file-extractor solo es válido en modo 'files'."})

    if action == "start":
        # Stop previous instance if any
        existing = running_workers.get(name, {}).get(worker)
        if existing and existing.poll() is None:
            return jsonify({"status": "already_running"})

        script = WORKER_SCRIPTS[worker]
        env = _build_worker_env(onto, worker, model=model)
        log_path = _get_worker_log_path(name, worker)
        
        # Open log file in append mode
        log_file = open(log_path, "a", encoding="utf-8")
        
        # Use creationflags to detach on windows if needed, but for now just redirect
        proc = subprocess.Popen(
            [sys.executable, f"/app/{script}"],
            env=env,
            stdout=log_file,
            stderr=log_file,
        )
        running_workers.setdefault(name, {})[worker] = proc
        logging.info(f"[WORKER] Iniciado {worker} para '{name}' (PID {proc.pid})")
        return jsonify({"status": "ok", "action": "started", "pid": proc.pid})

    elif action == "stop":
        proc = running_workers.get(name, {}).get(worker)
        if proc and proc.poll() is None:
            proc.terminate()
            try: proc.wait(timeout=5)
            except Exception: proc.kill()
        running_workers.get(name, {}).pop(worker, None)
        logging.info(f"[WORKER] Detenido {worker} para '{name}'")
        return jsonify({"status": "ok", "action": "stopped"})

    return jsonify({"status": "error", "msg": "Acción desconocida."})


# ── FILE UPLOAD ───────────────────────────────────────────────

@app.route('/api/ontologies/<name>/upload', methods=['POST'])
def api_upload_file(name):
    onto = get_ontology(name)
    if not onto:
        return jsonify({"status": "error", "msg": "Ontología no encontrada."})
    if onto["mode"] != "files":
        return jsonify({"status": "error", "msg": "Esta ontología no es de tipo 'files'."})

    files = request.files.getlist("files")
    if not files:
        return jsonify({"status": "error", "msg": "No se recibieron archivos."})

    saved = []
    for f in files:
        if f.filename == '':
            continue
        ext = os.path.splitext(f.filename)[1].lower()
        if ext not in ('.txt', '.pdf'):
            continue
        dest = os.path.join(onto["input_dir"], f.filename)
        f.save(dest)
        saved.append(f.filename)
    return jsonify({"status": "ok", "saved": saved})


# ── EXISTING EXPLORER ENDPOINTS (now onto-aware) ──────────────

def _get_onto_name_from_request():
    return request.args.get("onto")


@app.route('/api/entities')
def api_entities():
    onto_name = _get_onto_name_from_request()
    try:
        db = get_db(onto_name)
        res = db.run("?[e] := *eav[e, _, _, _, _, _]")
        entities = set(r[0] for r in res.get('rows', []))
        return jsonify(list(entities))
    except Exception:
        return jsonify([])


@app.route('/api/roots')
def api_roots():
    onto_name = _get_onto_name_from_request()
    try:
        db = get_db(onto_name)
        res = db.run("?[e, a, v] := *eav[e, a, v, _, _, _]")
        rows = res.get('rows', [])
        counter = collections.Counter(r[0] for r in rows)
        top = counter.most_common(20)
        return jsonify([{"concept": k, "relations": v} for k, v in top])
    except Exception:
        return jsonify([])


@app.route('/api/node/<path:concept>')
def api_node(concept):
    onto_name = _get_onto_name_from_request()
    try:
        db = get_db(onto_name)
        c_safe = concept.replace("'", "")
        out_res = db.run(f"?[a, v, c, s, b] := *eav[ent, a, v, c, s, b], ent = '{c_safe}'").get('rows', [])
        in_res  = db.run(f"?[e, a, c, s, b] := *eav[e, a, val, c, s, b], val = '{c_safe}'").get('rows', [])
        try:
            meta_res = db.run(f"?[d] := *concept_metadata['{c_safe}', d]").get('rows', [])
            description = meta_res[0][0] if meta_res else ""
        except Exception:
            description = ""
        return jsonify({
            "concept": concept, "description": description,
            "outgoing": [{"attribute": r[0], "target": r[1], "confidence": r[2], "source": r[3], "is_bind": r[4]} for r in out_res],
            "incoming": [{"source": r[0], "attribute": r[1], "confidence": r[2], "meta_source": r[3], "is_bind": r[4]} for r in in_res]
        })
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route('/api/rename', methods=['POST'])
def api_rename():
    try:
        data = request.json
        old_name = data.get('old_name')
        new_name = data.get('new_name')
        onto_name = data.get('onto')
        if not old_name or not new_name or old_name == new_name:
            return jsonify({"status": "error", "msg": "Nombres inválidos."})
        db = get_db(onto_name)
        c_old = old_name.replace("'", "")
        out_rows = db.run(f"?[a, v, c, s, b] := *eav[ent, a, v, c, s, b], ent = '{c_old}'").get('rows', [])
        in_rows  = db.run(f"?[e, a, c, s, b] := *eav[e, a, val, c, s, b], val = '{c_old}'").get('rows', [])
        rm_keys, put_rows = [], []
        for r in out_rows:
            rm_keys.append([c_old, r[0], r[1]])
            put_rows.append([new_name, r[0], r[1], r[2], r[3], r[4]])
        for r in in_rows:
            rm_keys.append([r[0], r[1], c_old])
            put_rows.append([r[0], r[1], new_name, r[2], r[3], r[4]])
        if rm_keys:
            db.run("?[entity, attribute, value] <- $data\n:rm eav {entity, attribute, value}", {"data": rm_keys})
        if put_rows:
            db.run("?[entity, attribute, value, confidence, source, is_bind] <- $data\n:put eav {entity, attribute, value => confidence, source, is_bind}", {"data": put_rows})
        meta_rows = db.run(f"?[c, d] := *concept_metadata['{c_old}', d]").get('rows', [])
        if meta_rows:
            desc = meta_rows[0][1]
            db.run("?[concept] <- $data\n:rm concept_metadata {concept}", {"data": [[old_name]]})
            db.run("?[concept, description] <- $data\n:put concept_metadata {concept => description}", {"data": [[new_name, desc]]})
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)})


# ── Q&A ───────────────────────────────────────────────────────

QUERY_PROMPT = """You are a CozoDB Datalog expert assistant. The database has two relations:
  *eav[entity: String, attribute: String, value: String, confidence: Float, source: String, is_bind: Bool]
  *concept_metadata[concept: String, description: String]

Common attributes: afecta_a, generado_por, compuesto_por, propuesto_por, es_un, requiere_de,
  es_instancia_de, afecta_indirectamente_a.

Given the user's question, write ONE single valid CozoDB Datalog query that answers it.
Return ONLY the raw Datalog query, no explanations, no markdown fences.

Examples:
Q: ¿Qué conceptos afectan a la Gravedad?
A: ?[e] := *eav[e, "afecta_a", "Gravedad", _, _, _]

Q: ¿Qué es la Mecánica Cuántica?
A: ?[d] := *concept_metadata["Mecánica Cuántica", d]

User question: {question}"""

ANSWER_PROMPT = """You are a scientific assistant. Answer the question in Spanish using ONLY the data provided.
If data is empty or error, say you don't have enough information. Be concise (2-4 sentences).
Question: {question}
Data from database: {data}"""


@app.route('/api/models')
def api_models():
    """Return list of models available in Ollama."""
    try:
        r = requests.get(f"{OLLAMA_HOST}/api/tags", timeout=10)
        r.raise_for_status()
        models = [m["name"] for m in r.json().get("models", [])]
        return jsonify(models)
    except Exception as ex:
        return jsonify([OLLAMA_MODEL])  # Fallback to default


def call_ollama(prompt, model=None):
    used_model = model or OLLAMA_MODEL
    payload = {"model": used_model, "prompt": prompt, "stream": False, "options": {"temperature": 0.1}}
    try:
        r = requests.post(f"{OLLAMA_HOST}/api/generate", json=payload, timeout=120)
        r.raise_for_status()
        return r.json().get("response", "").strip()
    except Exception as ex:
        return f"ERROR: {ex}"


@app.route('/api/ask', methods=['POST'])
def api_ask():
    data = request.json or {}
    question = data.get('question', '').strip()
    onto_name = data.get('onto')
    model = data.get('model') or None  # per-request model override
    if not question:
        return jsonify({"error": "No question provided."})
    # Step 1: generate Datalog query
    raw_query = call_ollama(QUERY_PROMPT.format(question=question), model=model)
    raw_query = raw_query.replace("```datalog", "").replace("```", "").strip()
    # Step 2: run on CozoDB
    db_results, query_error = [], None
    try:
        db = get_db(onto_name)
        res = db.run(raw_query)
        db_results = res.get('rows', [])
    except Exception as ex:
        query_error = str(ex)
    # Step 3: format answer in NL
    data_str = str(db_results) if db_results else (f"Query error: {query_error}" if query_error else "(vacío)")
    natural_answer = call_ollama(ANSWER_PROMPT.format(question=question, data=data_str), model=model)
    return jsonify({
        "question": question,
        "generated_query": raw_query,
        "raw_results": db_results,
        "answer": natural_answer,
        "query_error": query_error,
        "model_used": model or OLLAMA_MODEL
    })


# ── REVIEWS ───────────────────────────────────────────────────

@app.route('/api/pending_reviews')
def api_pending_reviews():
    onto_name = _get_onto_name_from_request()
    try:
        db = get_db(onto_name)
        res = db.run("?[id, oe, oa, ov, ne, na, nv, reason] := *pending_review[id, oe, oa, ov, ne, na, nv, reason]")
        rows = res.get('rows', [])
        return jsonify([{"id": r[0], "old_entity": r[1], "old_attr": r[2], "old_val": r[3],
                         "new_entity": r[4], "new_attr": r[5], "new_val": r[6], "reason": r[7]} for r in rows])
    except Exception:
        return jsonify([])


@app.route('/api/approve_review', methods=['POST'])
def api_approve_review():
    data = request.json or {}
    review_id = data.get('id')
    action = data.get('action')
    onto_name = data.get('onto')
    if not review_id or action not in ('approve', 'reject'):
        return jsonify({"status": "error", "msg": "Invalid params"})
    try:
        db = get_db(onto_name)
        res = db.run("?[id, oe, oa, ov, ne, na, nv, r] := *pending_review[id, oe, oa, ov, ne, na, nv, r], id=$id", {"id": review_id})
        rows = res.get('rows', [])
        if not rows:
            return jsonify({"status": "error", "msg": "Review not found"})
        r = rows[0]
        if action == 'approve':
            db.run("?[entity, attribute, value] <- $data\n:rm eav {entity, attribute, value}", {"data": [[r[1], r[2], r[3]]]})
            db.run("?[entity, attribute, value, confidence, source, is_bind] <- $data\n:put eav {entity, attribute, value => confidence, source, is_bind}",
                   {"data": [[r[4], r[5], r[6], 1.0, 'llm_reviewer', False]]})
        db.run("?[id] <- $data\n:rm pending_review {id}", {"data": [[review_id]]})
        return jsonify({"status": "ok"})
    except Exception as ex:
        return jsonify({"status": "error", "msg": str(ex)})


import collections

@app.route('/api/path')
def api_path():
    start_node = request.args.get('start')
    end_node   = request.args.get('end')
    onto_name  = request.args.get('onto')
    
    if not start_node or not end_node:
        return jsonify({"status": "error", "msg": "Faltan nodos de inicio o fin."})
    
    try:
        # Get custom depth, default 10, max 20
        try:
            max_depth = int(request.args.get('depth', 10))
        except:
            max_depth = 10
        max_depth = min(max_depth, 20)
        
        db = get_db(onto_name)
        # 1. Fetch all facts from the ontology (fast for manageable datasets)
        res = db.run("?[s, a, d] := *eav[s, a, d, _, _, _]")
        facts = res.get('rows', [])
        
        # 2. Build adjacency list (bi-directional)
        adj = collections.defaultdict(list)
        for s, a, d in facts:
            adj[s].append((s, a, d))
            adj[d].append((d, f"{a} (rev)", s))
            
        # 3. BFS search to find shortest paths up to max_depth
        # Format of queue items: (current_node, path_so_far)
        # path_so_far is a list of [src, attr, dst]
        queue = collections.deque([(start_node, [])])
        visited = {start_node: 0} # node -> shortest depth found
        found_paths = []
        max_paths = 20
        
        while queue:
            curr, path = queue.popleft()
            
            if len(path) >= max_depth:
                continue
                
            for s, a, d in adj.get(curr, []):
                # Cycle detection: only proceed if we haven't seen d at a shorter depth
                # (Allows multiple paths to d at the same minimum depth for diversity)
                if d in visited and visited[d] < len(path) + 1:
                    continue
                
                new_path = path + [[s, a, d]]
                
                if d == end_node:
                    found_paths.append(new_path)
                    if len(found_paths) >= max_paths:
                        # Found enough paths, we can stop
                        break
                else:
                    visited[d] = len(path) + 1
                    queue.append((d, new_path))
            
            if len(found_paths) >= max_paths:
                break
                
        # Sorted by length (BFS already finds them in increasing length order, but we ensure it)
        found_paths.sort(key=len)
        return jsonify({"status": "ok", "paths": found_paths})
    except Exception as ex:
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "msg": str(ex)})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)

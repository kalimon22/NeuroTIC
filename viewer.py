"""
viewer.py — NeuroTIC Web Explorer + API (Multi-Ontología)
Gestiona múltiples ontologías aisladas y sus workers desde la web.
"""
import os
import sys
import re
import json
import signal
import subprocess
import collections
import requests
import logging
import subprocess
from datetime import datetime
from flask import Flask, jsonify, render_template, request

import pycozo
from db_utils import setup_db, run_query
from ontology_manager import (
    load_registry, list_ontologies, get_ontology,
    create_ontology, delete_ontology
)
from wiki_utils import WikiManager

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
        concepts = 0
        grounded = 0
        if os.path.exists(o["db_path"]):
            try:
                db = get_db(o["name"])
                # Count relationships (facts) - Using head aggregator for compatibility
                res_f = run_query(db, "?[count(e)] := *eav[e,_,_,_,_,_]")
                facts = res_f.get('rows', [[0]])[0][0] if res_f.get('rows') else 0
                
                # Count unique concepts - Using multi-stage head aggregator
                res_c = run_query(db, "c[v] := *eav[v,_,_,_,_,_] c[v] := *eav[_,_,v,_,_,_] ?[count(v)] := c[v]")
                concepts = res_c.get('rows', [[0]])[0][0] if res_c.get('rows') else 0
                
                # Count grounded concepts (those in concept_metadata)
                res_g = run_query(db, "?[count(c)] := *concept_metadata[c, _]")
                grounded = res_g.get('rows', [[0]])[0][0] if res_g.get('rows') else 0
            except Exception:
                facts = 0
                concepts = 0
                grounded = 0

        workers_status = {}
        for wname in WORKER_SCRIPTS:
            worker_info = running_workers.get(o["name"], {}).get(wname)
            if worker_info is not None:
                proc = worker_info["proc"]
                if proc.poll() is None:
                    workers_status[wname] = {"status": "running", "model": worker_info["model"]}
                else:
                    workers_status[wname] = {"status": "stopped"}
            else:
                workers_status[wname] = {"status": "stopped"}

        result.append({**o, "facts": facts, "concepts": concepts, "grounded": grounded, "workers": workers_status})
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
    if worker_name in ("file-extractor", "grounding-engine"):
        env["EXTRACTOR_INPUT_DIR"] = onto["input_dir"]
        env["EXTRACTOR_DONE_DIR"]  = onto["done_dir"]
    if worker_name == "file-extractor":
        env["EXTRACTOR_CHUNK_SIZE"] = "1500"
    return env


def _get_worker_log_path(onto_name: str, worker_name: str) -> str:
    slug = onto_name.lower().replace(" ", "_")
    log_dir = "data/logs"
    os.makedirs(log_dir, exist_ok=True)
    return f"{log_dir}/{worker_name}_{slug}.log"


def _stop_all_workers(onto_name: str):
    for wname, worker_info in list(running_workers.get(onto_name, {}).items()):
        proc = worker_info["proc"]
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
        worker_info = running_workers.get(name, {}).get(wname)
        if worker_info is not None:
            proc = worker_info["proc"]
            if proc.poll() is None:
                result[wname] = {"status": "running", "model": worker_info["model"]}
            else:
                result[wname] = {"status": "stopped"}
        else:
            result[wname] = {"status": "stopped"}
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
        worker_info = running_workers.get(name, {}).get(worker)
        if worker_info and worker_info["proc"].poll() is None:
            return jsonify({"status": "already_running"})

        script = WORKER_SCRIPTS[worker]
        used_model = model or OLLAMA_MODEL
        env = _build_worker_env(onto, worker, model=used_model)
        log_path = _get_worker_log_path(name, worker)
        
        # Open log file in append mode
        log_file = open(log_path, "a", encoding="utf-8")
        
        # Use creationflags to detach on windows if needed, but for now just redirect
        # Redirecting to sys.stderr so it appears in the Docker console log
        proc = subprocess.Popen(
            [sys.executable, f"/app/{script}"],
            env=env,
            stdout=sys.stdout, 
            stderr=sys.stderr,
             # bufsize=0 is unbuffered
            bufsize=0
        )
        running_workers.setdefault(name, {})[worker] = {"proc": proc, "model": used_model}
        logging.info(f"[WORKER] Iniciado {worker} para '{name}' con modelo {used_model} (PID {proc.pid})")
        return jsonify({"status": "ok", "action": "started", "pid": proc.pid})

    elif action == "stop":
        worker_info = running_workers.get(name, {}).get(worker)
        if worker_info:
            proc = worker_info["proc"]
            if proc.poll() is None:
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
        if ext not in ('.txt', '.md', '.pdf'):
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
        query = "c[v] := *eav[v,_,_,_,_,_] c[v] := *eav[_,_,v,_,_,_] c[v] := *concept_metadata[v, _] ?[v] := c[v]"
        res = run_query(db, query)
        entities = set(r[0] for r in res.get('rows', []))
        return jsonify(list(entities))
    except Exception:
        return jsonify([])


@app.route('/api/roots')
def api_roots():
    onto_name = _get_onto_name_from_request()
    try:
        db = get_db(onto_name)
        res = run_query(db, "?[e, a, v] := *eav[e, a, v, _, _, _]")
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
        out_res = run_query(db, f"?[a, v, c, s, b] := *eav[ent, a, v, c, s, b], ent = '{c_safe}'").get('rows', [])
        in_res  = run_query(db, f"?[e, a, c, s, b] := *eav[e, a, val, c, s, b], val = '{c_safe}'").get('rows', [])
        try:
            meta_res = run_query(db, f"?[d] := *concept_metadata['{c_safe}', d]").get('rows', [])
            description = meta_res[0][0] if meta_res else ""
        except Exception:
            description = ""
            
        # ── WIKI CONTENT ──────────────────────────────────
        wiki_content = ""
        onto = get_ontology(onto_name)
        if onto:
            base_dir = os.path.dirname(onto["db_path"])
            wm = WikiManager(base_dir)
            wiki_content, _ = wm.read_page(concept)
            
        return jsonify({
            "concept": concept, "description": description,
            "markdown": wiki_content,
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
        out_rows = run_query(db, f"?[a, v, c, s, b] := *eav[ent, a, v, c, s, b], ent = '{c_old}'").get('rows', [])
        in_rows  = run_query(db, f"?[e, a, c, s, b] := *eav[e, a, val, c, s, b], val = '{c_old}'").get('rows', [])
        rm_keys, put_rows = [], []
        for r in out_rows:
            rm_keys.append([c_old, r[0], r[1]])
            put_rows.append([new_name, r[0], r[1], r[2], r[3], r[4]])
        for r in in_rows:
            rm_keys.append([r[0], r[1], c_old])
            put_rows.append([r[0], r[1], new_name, r[2], r[3], r[4]])
        if rm_keys:
            run_query(db, "?[entity, attribute, value] <- $data\n:rm eav {entity, attribute, value}", {"data": rm_keys})
        if put_rows:
            run_query(db, "?[entity, attribute, value, confidence, source, is_bind] <- $data\n:put eav {entity, attribute, value => confidence, source, is_bind}", {"data": put_rows})
        meta_rows = run_query(db, f"?[c, d] := *concept_metadata['{c_old}', d]").get('rows', [])
        if meta_rows:
            desc = meta_rows[0][1]
            run_query(db, "?[concept] <- $data\n:rm concept_metadata {concept}", {"data": [[old_name]]})
            run_query(db, "?[concept, description] <- $data\n:put concept_metadata {concept => description}", {"data": [[new_name, desc]]})
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)})


# ── Q&A ───────────────────────────────────────────────────────
QUERY_PROMPT = """RULES:
1. Start EVERY line with: ?[attr, val] :=
2. NEVER use other variables in the head. Always ?[attr, val].
3. Use multiple rules for OR logic.
4. Strings in "Double Quotes".

Examples:
Q: Qué conceptos afectan a la Gravedad?
A: ?[attr, val] := *eav[val, attr, "Gravedad", _, _, _], attr = "afecta_a"

Q: Qué sabes de Einstein?
A: 
?[attr, val] := *eav["Einstein", attr, val, _, _, _]
?[attr, val] := *eav[ent, attr, "Einstein", _, _, _], val = ent
?[attr, val] := *concept_metadata["Einstein", val], attr = "description"

Q: Qué es la Mecánica Cuántica?
A: ?[attr, val] := *concept_metadata["Mecánica Cuántica", val], attr = "description"

User question: {question}"""

HYBRID_PROMPT = """Eres un asistente científico experto. 
Debes responder a la pregunta del usuario utilizando dos fuentes de información:
1. DATOS ESTRUCTURADOS (del Grafo de Conocimiento): {data_db}
2. CONTEXTO NARRATIVO (del Wiki Markdown): {data_wiki}

Tu objetivo es ser preciso y explicativo. Si hay contradicciones, prioriza los datos estructurados, pero usa el Wiki para dar profundidad. 
Cita las fuentes (Wiki o Grafo) si es posible. Responde en español.

Pregunta: {question}"""


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
    model = data.get('model') or None
    mode = data.get('mode', 'datalog') # 'datalog' or 'wiki'

    if not question:
        return jsonify({"error": "No question provided."})

    context_wiki = ""
    res_db = []
    
    # ── SHARED STRATEGIES ──────────────────────────────────────
    def get_wiki_context(q, onto_n, mdl):
        onto = get_ontology(onto_n)
        if not onto: return ""
        wm = WikiManager(os.path.dirname(onto["db_path"]))
        idx = ""
        if os.path.exists(wm.index_path):
            with open(wm.index_path, "r", encoding="utf-8") as f: idx = f.read()
        sp = f"Índice del wiki:\n{idx}\nPregunta: {q}\nResponde SOLO los nombres de las 3 páginas más relevantes separadas por comas."
        pages = call_ollama(sp, model=mdl).split(",")
        ctx = ""
        for p in pages:
            c, _ = wm.read_page(p.strip().replace("[[", "").replace("]]", ""))
            if c: ctx += f"--- Wiki: {p.strip()} ---\n{c}\n\n"
        return ctx

    def get_db_results(q, onto_n, mdl):
        raw_r = call_ollama(QUERY_PROMPT.format(question=q), model=mdl)
        # Clean query (reuse the logic from before)
        lines = raw_r.split("\n")
        query_lines = []
        started = False
        for l in lines:
            l_strip = l.strip()
            if l_strip.startswith("?"): started = True
            if started:
                if any(c in l_strip for c in (':=', '*', '[', '|', ']', ',')): query_lines.append(l)
                elif len(l_strip.split()) > 3 and l_strip[0].isupper(): break
                else: break
        q_final = "\n".join(query_lines).strip()
        try:
            db = get_db(onto_n)
            return run_query(db, q_final).get('rows', []), q_final
        except Exception as e:
            return [], str(e)

    # ── MODES ──────────────────────────────────────────────────
    if mode == 'wiki':
        context_wiki = get_wiki_context(question, onto_name, model)
        final_prompt = f"Responde basándote solo en el Wiki:\nContexto:\n{context_wiki}\nPregunta: {question}"
        answer = call_ollama(final_prompt, model=model)
        return jsonify({"answer": answer, "mode": "wiki", "question": question})

    elif mode == 'datalog':
        res_db, q_run = get_db_results(question, onto_name, model)
        ans = call_ollama(f"Responde basándote en estos datos: {res_db}\nPregunta: {question}", model=model)
        return jsonify({"answer": ans, "mode": "datalog", "query": q_run, "question": question})

    elif mode == 'hybrid':
        context_wiki = get_wiki_context(question, onto_name, model)
        res_db, q_run = get_db_results(question, onto_name, model)
        
        final_p = HYBRID_PROMPT.format(
            question=question, 
            data_db=str(res_db), 
            data_wiki=context_wiki
        )
        answer = call_ollama(final_p, model=model)
        
        return jsonify({
            "answer": answer,
            "mode": "hybrid",
            "query": q_run,
            "db_facts": len(res_db),
            "question": question
        })


# ── REVIEWS ───────────────────────────────────────────────────

@app.route('/api/pending_reviews')
def api_pending_reviews():
    onto_name = _get_onto_name_from_request()
    try:
        db = get_db(onto_name)
        res = run_query(db, "?[id, oe, oa, ov, ne, na, nv, reason] := *pending_review[id, oe, oa, ov, ne, na, nv, reason]")
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
        res = run_query(db, "?[id, oe, oa, ov, ne, na, nv, r] := *pending_review[id, oe, oa, ov, ne, na, nv, r], id=$id", {"id": review_id})
        rows = res.get('rows', [])
        if not rows:
            return jsonify({"status": "error", "msg": "Review not found"})
        r = rows[0]
        if action == 'approve':
            run_query(db, "?[entity, attribute, value] <- $data\n:rm eav {entity, attribute, value}", {"data": [[r[1], r[2], r[3]]]})
            if r[4] != 'DELETE':
                run_query(db, "?[entity, attribute, value, confidence, source, is_bind] <- $data\n:put eav {entity, attribute, value => confidence, source, is_bind}",
                       {"data": [[r[4], r[5], r[6], 1.0, 'llm_reviewer', False]]})
        run_query(db, "?[id] <- $data\n:rm pending_review {id}", {"data": [[review_id]]})
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
        res = run_query(db, "?[s, a, d] := *eav[s, a, d, _, _, _]")
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

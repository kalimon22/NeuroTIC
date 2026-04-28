"""
ontology_manager.py — Registry de ontologías NeuroTIC
Gestiona el archivo registry.json que centraliza todas las ontologías.
"""
import os
import json
import shutil
from datetime import datetime
from wiki_utils import WikiManager

REGISTRY_PATH = os.environ.get("REGISTRY_PATH", "data/ontologies/registry.json")


def _ensure_dir():
    os.makedirs(os.path.dirname(REGISTRY_PATH), exist_ok=True)


def load_registry() -> dict:
    _ensure_dir()
    if not os.path.exists(REGISTRY_PATH):
        return {"ontologies": []}
    with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_registry(reg: dict):
    _ensure_dir()
    with open(REGISTRY_PATH, "w", encoding="utf-8") as f:
        json.dump(reg, f, indent=2, ensure_ascii=False)


def list_ontologies() -> list:
    return load_registry().get("ontologies", [])


def get_ontology(name: str) -> dict | None:
    for o in list_ontologies():
        if o["name"] == name:
            return o
    return None


def create_ontology(name: str, mode: str, seed: str = "", description: str = "") -> dict:
    """
    Create a new ontology entry.
    mode: "seed" | "files"
    """
    if get_ontology(name):
        raise ValueError(f"Ya existe una ontología con el nombre '{name}'")

    slug = name.lower().replace(" ", "_")
    base_dir = f"data/ontologies/{slug}"
    db_path = f"{base_dir}/ontology.db"
    input_dir = f"{base_dir}/input_files"
    done_dir = f"{input_dir}/done"

    os.makedirs(base_dir, exist_ok=True)
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(done_dir, exist_ok=True)

    # Initialize Wiki structure
    WikiManager(base_dir)

    entry = {
        "name": name,
        "slug": slug,
        "mode": mode,
        "seed": seed if mode == "seed" else "",
        "description": description,
        "db_path": db_path,
        "input_dir": input_dir,
        "done_dir": done_dir,
        "wiki_dir": f"{base_dir}/wiki",
        "created_at": datetime.utcnow().isoformat()
    }

    reg = load_registry()
    reg["ontologies"].append(entry)
    save_registry(reg)
    return entry


def delete_ontology(name: str) -> bool:
    reg = load_registry()
    orig = reg["ontologies"]
    entry = get_ontology(name)
    if not entry:
        return False
    reg["ontologies"] = [o for o in orig if o["name"] != name]
    save_registry(reg)
    # Remove the directory
    base_dir = os.path.dirname(entry["db_path"])
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)
    return True

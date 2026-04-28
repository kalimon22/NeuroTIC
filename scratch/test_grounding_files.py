import os
import pycozo
from db_utils import run_query
import glob

DB_PATH = os.environ.get("DB_PATH", "data/ontology.db")

def test():
    db_path = DB_PATH
    if not os.path.exists(db_path):
        print(f"DB not found at {db_path}")
        return
        
    db = pycozo.Client('sqlite', db_path, dataframe=False)
    
    query = """
    all_concepts[c] := *eav[c, _, _, _, _, _]
    all_concepts[c] := *eav[_, _, c, _, _, _]
    grounded[c] := *concept_metadata[c, _]
    ?[pending] := all_concepts[pending], not grounded[pending]
    :limit 5
    """
    res = run_query(db, query)
    concepts = [r[0] for r in res.get('rows', [])]
    
    print(f"Pending concepts: {concepts}")
    
    for c in concepts:
        q2 = """
        ?[source] := *eav[$c, _, _, _, source, _]
        ?[source] := *eav[_, _, $c, _, source, _]
        """
        r2 = run_query(db, q2, {"c": c})
        sources = [r[0] for r in r2.get('rows', [])]
        print(f"Concept: {c}, Sources: {sources}")
        
        # Test file lookup
        input_dir = os.environ.get("EXTRACTOR_INPUT_DIR", "data/input_files")
        done_dir = os.environ.get("EXTRACTOR_DONE_DIR", "data/input_files/done")
        
        print(f"Looking in: {input_dir} and {done_dir}")
        print(f"Files in {input_dir}: {glob.glob(input_dir + '/*')}")
        print(f"Files in {done_dir}: {glob.glob(done_dir + '/*')}")

if __name__ == "__main__":
    test()

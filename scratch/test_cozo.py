import pycozo
import os

DB_PATH = r"C:\data\ontologies\einstein\ontology.db"

def test_query(q):
    print(f"\n--- Testing Query ---\n{q}")
    try:
        db = pycozo.Client('sqlite', DB_PATH, dataframe=False)
        res = db.run(q)
        print("SUCCESS!")
        print("Rows:", res.get('rows', []))
    except Exception as e:
        print("FAILED!")
        print("Error:", str(e))

# Test 1: The one the LLM generated (should fail)
test_query("""
?[attr, val] := *eav["Einstein", attr, val, _, _, _]
?[attr, val] := *eav[entity, attr, "Einstein", _, _, _], val = entity
?[desc] := *concept_metadata["Einstein", desc]
""")

# Test 2: The fixed one I want (should succeed)
test_query("""
?[attr, val] := *eav["Einstein", attr, val, _, _, _]
?[attr, val] := *eav[ent, attr, "Einstein", _, _, _], val = ent
?[attr, val] := *concept_metadata["Einstein", val], attr = "description"
""")

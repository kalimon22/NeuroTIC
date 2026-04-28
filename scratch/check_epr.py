import pycozo
import os

db_path = r'C:\data\ontologies\einstein\ontology.db'
if os.path.exists(db_path):
    print(f"Opening DB at {db_path}")
    db = pycozo.Client('sqlite', db_path, dataframe=False)
    # Search in entities and values
    res = db.run("c[v] := *eav[v,_,_,_,_,_] c[v] := *eav[_,_,v,_,_,_] ?[v] := c[v]")
    rows = res.get('rows', [])
    matches = [r[0] for r in rows if 'epr' in str(r[0]).lower()]
    print(f"Matches found in EAV: {matches}")
    
    # Check metadata specifically
    res_meta = db.run("?[c] := *concept_metadata[c, _]")
    meta_rows = [r[0] for r in res_meta.get('rows', []) if 'epr' in str(r[0]).lower()]
    print(f"Metadata matches: {meta_rows}")
else:
    print(f"Database file not found at {db_path}")

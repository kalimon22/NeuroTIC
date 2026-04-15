
import pycozo
import os
import json

with open('data/ontologies/registry.json', 'r') as f:
    ontos = json.load(f).get('ontologies', [])

for o in ontos:
    print(f"\n--- Checking {o['name']} ---")
    path = o['db_path']
    print(f"Path: {path} (Exists: {os.path.exists(path)})")
    if os.path.exists(path):
        try:
            db = pycozo.Client('sqlite', path, dataframe=False)
            
            # Test facts count
            res_f = db.run("?[n] := *eav[e,a,v,_,_,_], n = count(e,a,v)")
            print(f"Facts Query Result: {res_f}")
            
            # Test simple count
            res_s = db.run("?[count(e, a, v)] := *eav[e, a, v, _, _, _]")
            print(f"Simple Count Result: {res_s}")
            
        except Exception as e:
            print(f"ERROR: {e}")

import os
import json
import argparse




parser = argparse.ArgumentParser()
parser.add_argument("--dir", type=str, help="/directory/to/dbt/project/folder")
args = parser.parse_args()

os.chdir(os.path.expanduser('~'))
dir = args.dir
search_str = 'n=[o("manifest","manifest.json"+t),o("catalog","catalog.json"+t)]'

with open(dir + '/target/index.html', 'r') as f:
    content_index = f.read()
    
with open(dir + '/target/manifest.json', 'r') as f:
    json_manifest = json.loads(f.read())

with open(dir + '/target/catalog.json', 'r') as f:
    json_catalog = json.loads(f.read())
    
with open(dir + '/target/dbt_data_catalog.html', 'w') as f:
    new_str = "n=[{label: 'manifest', data: "+json.dumps(json_manifest)+"},{label: 'catalog', data: "+json.dumps(json_catalog)+"}]"
    new_content = content_index.replace(search_str, new_str)
    f.write(new_content)

print("[INFO] Combined dbt data catalog files.")
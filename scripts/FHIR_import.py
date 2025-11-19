## This script is using python to load multiple files in JSON format to MS SQL Server 
import pymssql
import os
import json
from glob import glob

# === CONFIG ===
SERVER = "########"
PORT = 1433
DATABASE = "DataWarehouse_HC"
USERNAME = "sa"
PASSWORD = "***********"

FOLDER_PATH = r"C:\Users\Sunshine\Desktop\DA Projects\ACB project\synthetic_canadians_fhir_12may2021\fhir"

INSERT_SQL = """
INSERT INTO RawFHIRBundle (FullUrl, ResourceType, ResourceJSON)
VALUES (%s, %s, %s)
"""

# === CONNECT ===
conn = pymssql.connect(
    server=SERVER,
    port=PORT,
    user=USERNAME,
    password=PASSWORD,
    database=DATABASE
)
cursor = conn.cursor()
print("✅ Connected to SQL Server.")

# === LOOP THROUGH FILES ===
for file_path in glob(os.path.join(FOLDER_PATH, "*.json")):
    print(f"📂 Processing file: {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            bundle = json.load(f)

        # Each file is a FHIR Bundle
        for entry in bundle.get('entry', []):
            full_url = entry.get('fullUrl')
            resource = entry.get('resource', {})
            resource_type = resource.get('resourceType', 'Unknown')
            resource_json = json.dumps(resource)

            cursor.execute(INSERT_SQL, (full_url, resource_type, resource_json))

        conn.commit()
        print(f"✔ Successfully loaded {file_path}")
    except Exception as e:
        print(f"❌ Failed to load {file_path}: {e}")

# === CLEAN UP ===
cursor.close()
conn.close()
print("\n🎉 All files processed.")

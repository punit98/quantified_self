"""



"""



import requests
import os
import base64
from dotenv import load_dotenv

load_dotenv()

# Connection Details
DATABRICKS_HOST = "https://dbc-22d2b1a2-c7c9.cloud.databricks.com/"
WORKSPACE_PATH = "/Workspace/Users/punit98@protonmail.com/quantified_self/.env.private"
LOCAL_ENV_FILE = ".env.private"


# Authentication
DATABRICKS_TOKEN = os.getenv("databricks_PAT")

if not DATABRICKS_TOKEN:
    raise ValueError("Set DATABRICKS_TOKEN in your environment")




# upload the file

with open(LOCAL_ENV_FILE, "rb") as f:
    content = f.read()
    b64_content = base64.b64encode(content).decode("utf-8")

payload = {
    "path": WORKSPACE_PATH,
    "overwrite": True,
    "format": "AUTO",
    "content": b64_content.encode("utf-8").decode("utf-8")
}

resp = requests.post(
    f"{DATABRICKS_HOST}/api/2.0/workspace/import",
    headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
    json=payload
)

if resp.status_code == 200:
    print("Uploaded .env.private successfully")
else:
    print("Upload failed:", resp.text)

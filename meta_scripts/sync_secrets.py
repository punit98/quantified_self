"""
This file syncs the secrets stored in .env.private file directly 
to your darabricks workspace folder without committing the .env.private
file to the repo. 

Follow the steps to make it work:
1. Create `.env.private` file in the root folder
2. Add all the personal details into the `env.private` folder like `date_of_birth`, `address`, `weight` etc
3. Add your databricks `secrets` Personal Access Token to your `.env` file in the root folder 
(it is separate from the `.env.private` and `.databricks.cfg` files because it contains 
the PAT to manage your secrets and never leaves your local machine)
4. Update system paths in `quantified_self/utilities/project_paths.py`
5. Once all the secrets in `.env.private` are added and `project_paths.py` file is updated, 
run this script locally and your `.env.private` file will be synced to your databricks workspace without goint through git


"""



import requests
import os
import base64
from dotenv import load_dotenv
from project_utilities import project_paths

load_dotenv()




# Authentication
DATABRICKS_TOKEN = os.getenv("databricks_PAT")

if not DATABRICKS_TOKEN:
    raise ValueError("Set DATABRICKS_TOKEN in your environment")




# upload the file

with open(project_paths.LOCAL_ENV_FILE, "rb") as f:
    content = f.read()
    b64_content = base64.b64encode(content).decode("utf-8")

payload = {
    "path": project_paths.WORKSPACE_PATH,
    "overwrite": True,
    "format": "AUTO",
    "content": b64_content.encode("utf-8").decode("utf-8")
}

resp = requests.post(
    f"{project_paths.DATABRICKS_HOST}/api/2.0/workspace/import",
    headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
    json=payload
)

if resp.status_code == 200:
    print("Uploaded .env.private successfully")
else:
    print("Upload failed:", resp.text)

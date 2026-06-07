from pathlib import Path

from databricks.sdk import WorkspaceClient
from dotenv import dotenv_values

# .env.private lives in the project root, one level up from meta_scripts/
env_path = ".env.private"
config = dotenv_values(env_path)

if not config:
    raise SystemExit(f"No secrets loaded from {env_path}")

# WorkspaceClient auto-authenticates from the named profile in ~/.databrickscfg
w = WorkspaceClient(profile="Default")

for key, value in config.items():
    w.secrets.put_secret(scope="personal-details", key=key.lower(), string_value=value)
    print(f"  synced {key.lower()}")

print("Secrets synced.")



import os

env_path = "/Workspace/Users/punit98@protonmail.com/quantified_self/.env.private"

def load_env(path):
    with open(path) as f:
        for line in f:
            if "=" in line:
                key, value = line.strip().split("=", 1)
                os.environ[key] = value

load_env(env_path)

date_of_birth = os.environ["date_of_birth"]
height_cm = os.environ["height_cm"]

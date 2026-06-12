import os

env_path = "/Workspace/Users/punit98@protonmail.com/quantified_self/.env.private"


def load_env(path):
    with open(path) as f:
        for line in f:
            if "=" in line:
                key, value = line.strip().split("=", 1)
                os.environ[key.strip()] = value.strip().strip('"')


load_env(env_path)

DATE_OF_BIRTH = os.environ["DATE_OF_BIRTH"]
HEIGHT_CM = os.environ["HEIGHT_CM"]
EMAIL = os.environ("EMAIL")

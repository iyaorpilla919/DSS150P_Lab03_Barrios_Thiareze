import os
from pathlib import Path
import yaml
from dotenv import load_dotenv

load_dotenv()
BASE_DIR = Path(__file__).resolve().parent.parent

def load_settings():
    with open(BASE_DIR / "config" / "settings.yml") as f:
        settings = yaml.safe_load(f)
    settings["postgres"] = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "db": os.getenv("POSTGRES_DB", "dss150p"),
        "user": os.getenv("POSTGRES_USER", "dss150p"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }
    return settings

SETTINGS = load_settings()

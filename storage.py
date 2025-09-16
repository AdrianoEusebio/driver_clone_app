import json, os, time, uuid, tempfile, shutil
from typing import Any, Optional

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def atomic_write_json(path: str, data: Any):
    dname = os.path.dirname(path)
    ensure_dir(dname)
    fd, tmppath = tempfile.mkstemp(prefix=".tmp-", dir=dname)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmppath, path)

def read_json(path: str, default: Any):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default

def new_job_id() -> str:
    return uuid.uuid4().hex

def utc_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

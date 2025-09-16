import os, math, time, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Tuple, List
from googleapiclient.errors import HttpError

from storage import read_json, atomic_write_json, utc_iso
from drive_api import get_creds, drive_service, list_children, ensure_folder, copy_file_server_side, trash_if_exists

WORKERS = int(os.getenv("WORKERS", "6"))
MODE_REPLACE = os.getenv("MODE_REPLACE", "trash_then_copy")  # ou 'update_media'
HANDLE_SHORTCUTS = os.getenv("HANDLE_SHORTCUTS", "copy_as_shortcut")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))

def should_copy(prev_meta: dict, src: dict) -> bool:
    if not prev_meta: return True
    # compara por modifiedTime e md5 (quando houver)
    if src.get("md5Checksum") and prev_meta.get("md5Checksum"):
        if src["md5Checksum"] != prev_meta["md5Checksum"]:
            return True
    return src.get("modifiedTime") != prev_meta.get("modifiedTime")

def load_state(state_path: str) -> dict:
    return read_json(state_path, default={})

def save_state(state_path: str, state: dict):
    atomic_write_json(state_path, state)

def update_job(job_path: str, patch: dict):
    job = read_json(job_path, {})
    job.update(patch)
    job["updated_at"] = utc_iso()
    atomic_write_json(job_path, job)

def scan_tree(drive, src_root: str) -> List[dict]:
    # BFS simples
    queue = [src_root]
    out = []
    while queue:
        folder_id = queue.pop(0)
        for it in list_children(drive, folder_id):
            it["_parent"] = folder_id
            out.append(it)
            if it["mimeType"] == "application/vnd.google-apps.folder":
                queue.append(it["id"])
    return out

def build_plan(drive, state: dict, src_root: str, dest_root: str) -> Tuple[List[dict], int, int]:
    """
    Retorna (acoes_de_arquivo, items_total, bytes_total)
    Cria pastas antecipadamente (garantindo espelho).
    """
    items = scan_tree(drive, src_root)
    # map de pastas destino conhecidas no state: state['folders'][src_id] = dest_id
    folders = state.setdefault("folders", {})
    files_meta = state.setdefault("files", {})

    # primeiro, criar/garantir pastas
    for it in items:
        if it["mimeType"] == "application/vnd.google-apps.folder":
            parent_dest = folders.get(it["_parent"], dest_root if it["_parent"] == src_root else None)
            if parent_dest is None:
                parent_dest = folders.get(src_root, dest_root)
            dest_id = ensure_folder(drive, parent_dest, it["name"])
            folders[it["id"]] = dest_id

    # depois, decidir arquivos
    plan = []
    items_total = 0
    bytes_total = 0
    for it in items:
        if it["mimeType"] == "application/vnd.google-apps.folder":
            items_total += 1
            continue
        # shortcuts: por padrão, copiar como atalho
        if it["mimeType"] == "application/vnd.google-apps.shortcut" and HANDLE_SHORTCUTS == "copy_as_shortcut":
            items_total += 1
            continue  # (opcional: criar atalho igual no destino)
        parent_dest = folders.get(it["_parent"], dest_root)
        prev = files_meta.get(it["id"])
        if should_copy(prev, it):
            size = int(it.get("size", "0") or "0")
            bytes_total += size
            plan.append({"src": it, "dest_parent": parent_dest, "size": size})
        items_total += 1

    return plan, items_total, bytes_total

def run_job(data_dir: str, job_path: str, state_path: str):
    # lê job
    job = read_json(job_path, {})
    src = job["src_id"]
    dest = job["dest_id"]
    repeat_every = int(job.get("repeat_interval_min") or 0)

    while True:
        update_job(job_path, {"status": "planning", "message": "Conectando ao Drive..."})
        creds = get_creds(data_dir)
        drive = drive_service(creds)

        update_job(job_path, {"message": "Carregando state..."})
        state = load_state(state_path)

        # garantir pasta raiz de destino mapeada
        folders = state.setdefault("folders", {})
        if src not in folders:
            folders[src] = dest  # raiz: mapear SRC->DEST

        update_job(job_path, {"message": "Escaneando origem..."})
        plan, items_total, bytes_total = build_plan(drive, state, src, dest)

        progress = {"items_done": 0, "items_total": items_total, "bytes_done": 0, "bytes_total": bytes_total, "percent_items": 0.0, "percent_bytes": 0.0}
        update_job(job_path, {"status": "running", "progress": progress, "message": f"{len(plan)} arquivos a copiar"})

        files_meta = state.setdefault("files", {})
        cancel_flag = lambda: read_json(job_path, {}).get("cancel_requested") is True

        def work_one(item):
            if cancel_flag():
                return ("canceled", 0)
            src_file = item["src"]
            parent_id = item["dest_parent"]
            size = item["size"]
            # se existir mapeamento antigo (para MODE_REPLACE), apenas trash e recopie
            # (exemplo simples: sempre copy server-side)
            attempts = 0
            while True:
                try:
                    copy_id = copy_file_server_side(drive, src_file["id"], parent_id, name=src_file["name"])
                    # atualizar state
                    files_meta[src_file["id"]] = {
                        "destId": copy_id,
                        "modifiedTime": src_file.get("modifiedTime"),
                        "md5Checksum": src_file.get("md5Checksum"),
                        "size": src_file.get("size"),
                    }
                    return ("ok", size)
                except HttpError as e:
                    attempts += 1
                    if attempts >= MAX_RETRIES:
                        return ("error", 0)
                    time.sleep(2 ** attempts)

        # criar pastas já foi feito no build_plan
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = [pool.submit(work_one, it) for it in plan]
            for fut in as_completed(futures):
                status, added = fut.result()
                if status == "error":
                    update_job(job_path, {"status": "error", "message": "Falha ao copiar um arquivo. Ver logs.",})
                    save_state(state_path, state)
                    return
                if status == "canceled":
                    update_job(job_path, {"status": "canceled", "message": "Cancelado pelo usuário."})
                    save_state(state_path, state)
                    return
                progress["items_done"] += 1
                progress["bytes_done"] += added
                progress["percent_items"] = round(100.0 * progress["items_done"] / max(progress["items_total"],1), 2)
                if progress["bytes_total"] > 0:
                    progress["percent_bytes"] = round(100.0 * progress["bytes_done"] / progress["bytes_total"], 2)
                update_job(job_path, {"progress": progress})

        save_state(state_path, state)
        update_job(job_path, {"status": "completed", "message": "Sincronização concluída.", "ended_at": utc_iso()})

        if repeat_every > 0:
            for _ in range(repeat_every * 60):
                if cancel_flag():
                    update_job(job_path, {"status": "canceled", "message": "Agendamento cancelado."})
                    return
                time.sleep(1)
            # loop novamente (repetição)
            update_job(job_path, {"status": "queued", "message": f"Reiniciando job agendado a cada {repeat_every} min..."})
            continue

        return

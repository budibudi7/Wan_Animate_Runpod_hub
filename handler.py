import runpod
from runpod.serverless.utils import rp_upload

import os
import json
import uuid
import time
import logging
import websocket
import urllib.request
import urllib.parse
import subprocess
import base64
import binascii

# ======================
# LOGGING
# ======================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ======================
# GLOBAL CONFIG
# ======================
SERVER_ADDRESS = os.getenv("SERVER_ADDRESS", "127.0.0.1")
CLIENT_ID = str(uuid.uuid4())

# ======================
# UTIL FUNCTIONS
# ======================
def load_workflow(path: str):
    with open(path, "r") as f:
        return json.load(f)

def queue_prompt(prompt):
    url = f"http://{SERVER_ADDRESS}:8188/prompt"
    payload = {
        "prompt": prompt,
        "client_id": CLIENT_ID
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data)
    return json.loads(urllib.request.urlopen(req).read())

def get_history(prompt_id):
    url = f"http://{SERVER_ADDRESS}:8188/history/{prompt_id}"
    with urllib.request.urlopen(url) as response:
        return json.loads(response.read())

def download_file_from_url(url, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    result = subprocess.run(
        ["wget", "-O", output_path, "--no-verbose", url],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Download failed: {result.stderr}")
    return output_path

def save_base64_to_file(base64_data, output_path):
    try:
        decoded = base64.b64decode(base64_data)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "wb") as f:
            f.write(decoded)
        return output_path
    except (binascii.Error, ValueError) as e:
        raise Exception(f"Base64 decode failed: {e}")

def process_input(value, task_dir, filename, input_type):
    path = os.path.join(task_dir, filename)

    if input_type == "path":
        return value
    elif input_type == "url":
        return download_file_from_url(value, path)
    elif input_type == "base64":
        return save_base64_to_file(value, path)
    else:
        raise Exception(f"Unsupported input type: {input_type}")

# ======================
# COMFY EXECUTION
# ======================
def get_videos(ws, prompt):
    prompt_id = queue_prompt(prompt)["prompt_id"]
    logger.info(f"Prompt queued: {prompt_id}")

    while True:
        msg = ws.recv()
        if isinstance(msg, str):
            msg = json.loads(msg)
            if msg.get("type") == "executing":
                data = msg.get("data", {})
                if data.get("node") is None and data.get("prompt_id") == prompt_id:
                    break

    history = get_history(prompt_id)[prompt_id]
    output_urls = []

    for node in history["outputs"].values():
        if "gifs" not in node:
            continue

        for video in node["gifs"]:
            logger.info(f"Uploading video: {video['fullpath']}")
            url = rp_upload(
                file_path=video["fullpath"],
                key=f"outputs/{uuid.uuid4()}.mp4"
            )
            output_urls.append(url)

    return output_urls

# ======================
# HANDLER
# ======================
def handler(job):
    job_input = job.get("input", {})
    logger.info(f"Received job input: {job_input}")

    task_dir = f"/tmp/task_{uuid.uuid4()}"
    prompt = None  # 🔥 IMPORTANT FIX

    # ------------------
    # INPUT FILES
    # ------------------
    image_path = None
    video_path = None

    if "image_path" in job_input:
        image_path = process_input(job_input["image_path"], task_dir, "input.jpg", "path")
    elif "image_url" in job_input:
        image_path = process_input(job_input["image_url"], task_dir, "input.jpg", "url")
    elif "image_base64" in job_input:
        image_path = process_input(job_input["image_base64"], task_dir, "input.jpg", "base64")

    if "video_path" in job_input:
        video_path = process_input(job_input["video_path"], task_dir, "input.mp4", "path")
    elif "video_url" in job_input:
        video_path = process_input(job_input["video_url"], task_dir, "input.mp4", "url")
    elif "video_base64" in job_input:
        video_path = process_input(job_input["video_base64"], task_dir, "input.mp4", "base64")

    # ------------------
    # WORKFLOW SELECT
    # ------------------
    has_points = job_input.get("points_store") is not None
    is_animate = job_input.get("mode", "replace") == "animate"

    if not has_points:
        prompt = load_workflow(
            "/newWanAnimate_noSAM_animate_api.json"
            if is_animate else
            "/newWanAnimate_noSAM_api.json"
        )
    else:
        prompt = load_workflow(
            "/newWanAnimate_point_animate_api.json"
            if is_animate else
            "/newWanAnimate_point_api.json"
        )

    if prompt is None:
        raise Exception("Prompt workflow failed to load")

    # ------------------
    # PROMPT PARAMS
    # ------------------
    prompt["57"]["inputs"]["image"] = image_path
    prompt["63"]["inputs"]["video"] = video_path
    prompt["63"]["inputs"]["force_rate"] = job_input["fps"]
    prompt["30"]["inputs"]["frame_rate"] = job_input["fps"]
    prompt["65"]["inputs"]["positive_prompt"] = job_input["prompt"]
    prompt["65"]["inputs"]["negative_prompt"] = job_input.get("negative_prompt", "")
    prompt["27"]["inputs"]["seed"] = job_input["seed"]
    prompt["27"]["inputs"]["cfg"] = job_input["cfg"]
    prompt["27"]["inputs"]["steps"] = job_input.get("steps", 4)
    prompt["150"]["inputs"]["value"] = job_input["width"]
    prompt["151"]["inputs"]["value"] = job_input["height"]

    if has_points:
        prompt["107"]["inputs"]["points_store"] = job_input["points_store"]
        prompt["107"]["inputs"]["coordinates"] = job_input["coordinates"]
        prompt["107"]["inputs"]["neg_coordinates"] = job_input["neg_coordinates"]

    # ------------------
    # CONNECT COMFY
    # ------------------
    ws_url = f"ws://{SERVER_ADDRESS}:8188/ws?clientId={CLIENT_ID}"

    ws = websocket.WebSocket()
    ws.connect(ws_url)

    try:
        video_urls = get_videos(ws, prompt)
    finally:
        ws.close()

    if not video_urls:
        return {"status": "error", "message": "No video generated"}

    return {
        "status": "ok",
        "video_url": video_urls[0]
    }

# ======================
# START SERVERLESS
# ======================
runpod.serverless.start({"handler": handler})

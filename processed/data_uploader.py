import os
import requests
import base64
import json
from config import OUTPUT_DATA_DIR, TOKEN


def _get_file_sha(url, headers):
    """Check if file exists on GitHub and return its sha (needed to update existing files)"""
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get("sha")
    return None


def upload_json(batch_num, data):
    """
    Upload batch data as JSON to private GitHub repo output folder.
    Filename is derived from batch_num tracked in progress.json.

    Args:
        batch_num (int): Current batch number from progress tracking
        data (dict): Batch data to upload as JSON
    """
    token = TOKEN
    if not token:
        raise ValueError("TOKEN not found in environment variables")

    filename = f"batch-{batch_num}.json"
    url = f"{OUTPUT_DATA_DIR}{filename}"

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }

    # Get sha if file already exists (GitHub requires sha to overwrite)
    sha = _get_file_sha(url, headers)
    if sha:
        print(f"📄 File exists, updating: {filename}")
    else:
        print(f"📄 New file, creating: {filename}")

    content_str = json.dumps(data, indent=2)
    encoded = base64.b64encode(content_str.encode("utf-8")).decode("utf-8")

    payload = {
        "message": f"Upload {filename} (batch-{batch_num})",
        "content": encoded
    }
    if sha:
        payload["sha"] = sha

    response = requests.put(url, headers=headers, data=json.dumps(payload))

    if response.status_code in (200, 201):
        print(f"✅ Uploaded: {filename} → {UPLOAD_DATA_FOLDER}")
    else:
        raise Exception(
            f"❌ Upload failed for {filename}: "
            f"{response.status_code} {response.text}"
        )

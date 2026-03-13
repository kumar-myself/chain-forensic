import os
import requests
import base64
import pandas as pd
from io import StringIO

def load_csv():
    url = os.environ.get("SOURCE_DATA_URL")
    token = os.environ.get("TOKEN")

    if not url:
        raise ValueError("SOURCE_DATA_URL not found in environment variables")
    if not token:
        raise ValueError("TOKEN not found in environment variables")

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }

    response = requests.get(url, headers=headers)
    print("HTTP Status:", response.status_code)
    res = response.json()

    if "content" not in res:
        raise Exception(f"GitHub API error: {res}")

    encoding = res.get("encoding", "unknown")
    print("Encoding:", encoding)
    print("Content length:", len(res["content"]))

    # Handle large files (>1MB) where GitHub returns empty content
    if encoding == "none" or not res["content"].strip():
        download_url = res.get("download_url")
        if not download_url:
            raise Exception("File too large for Contents API and no download_url available")
        print("File too large, downloading via download_url...")
        raw_response = requests.get(download_url, headers=headers)
        raw_response.raise_for_status()
        content = raw_response.text
    else:
        content = base64.b64decode(res["content"].replace("\n", "")).decode("utf-8")

    if not content.strip():
        raise Exception("Decoded content is empty — check the file has data")

    df = pd.read_csv(StringIO(content))
    print("CSV loaded successfully, Rows:", len(df))
    return df

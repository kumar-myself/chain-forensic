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
        "Authorization": f"token {token}"
    }

    response = requests.get(url, headers=headers)

    print("HTTP Status:", response.status_code)

    res = response.json()

    # check GitHub API error
    if "content" not in res:
        raise Exception(f"GitHub API error: {res}")

    content = base64.b64decode(res["content"].replace("\n", "")).decode("utf-8")

    df = pd.read_csv(StringIO(content))

    print("CSV loaded successfully")
    print("Rows:", len(df))

    return df

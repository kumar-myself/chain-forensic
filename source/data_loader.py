import os
import requests
import base64
import pandas as pd
from io import StringIO


def load_csv():
    url = os.environ["DATA_URL"]
    token = os.environ["TOKEN"]

    headers = {
        "Authorization": f"token {token}"
    }

    response = requests.get(url, headers=headers)
    res = response.json()

    content = base64.b64decode(res["content"]).decode("utf-8")

    df = pd.read_csv(StringIO(content))

    return df

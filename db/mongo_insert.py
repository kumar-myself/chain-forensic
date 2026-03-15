import hashlib
import orjson
from urllib.parse import quote_plus
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import BulkWriteError
from config import MONGO_USER, MONGO_PASSWORD, MONGO_HOST, MONGO_DB, MONGO_COLLECTION

# ============================================
# HASH
# ============================================

HASH_FIELDS = [
    "category",
    "description",
    "loss_amount",
    "addresses",
    "domains",
    "total_addresses",
    "total_domains"
]

def normalize_value(v):
    if isinstance(v, list):
        return sorted(v)
    return v

def create_hash(doc):
    payload = {}
    for field in HASH_FIELDS:
        if field in doc:
            payload[field] = doc[field]
    binary = orjson.dumps(payload)
    return hashlib.sha256(binary).hexdigest()

# ============================================
# MONGO
# ============================================

password = quote_plus(MONGO_PASSWORD)
uri = f"mongodb+srv://{MONGO_USER}:{password}@{MONGO_HOST}/?appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

db = client[MONGO_DB]
col = db[MONGO_COLLECTION]

# ============================================
# INSERT
# ============================================

def insert_reports(reports: list):
    batch = []
    for doc in reports:
        doc["hash"] = create_hash(doc)
        
        # Add category_tag at birth
        if doc.get("category"):
            doc["category_tag"] = doc["category"].split(":")[0].strip()
        
        batch.append(doc)
    
    try:
        col.insert_many(batch, ordered=False)
    except BulkWriteError as e:
        inserted = e.details["nInserted"]
        print("Inserted:", inserted)
        print("Failed:", len(e.details["writeErrors"]))

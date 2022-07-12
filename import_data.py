import json
import os
import pathlib
import random
import time
from datetime import timedelta

import requests
import urllib3

# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.management.collections import CollectionSpec

# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import ClusterOptions, ClusterTimeoutOptions
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from tqdm import tqdm

# Disable certificate warnings
urllib3.disable_warnings()

# Load Environment Variables and Defaults
load_dotenv()
endpoint = os.getenv("DB_HOST")
username = os.getenv("DB_USER")
password = os.getenv("DB_PASS")
bucket_name = os.getenv("BUCKET")
scope_name = os.getenv("SCOPE")
data_directory = os.getenv("DATA_DIR")
data_host = os.getenv("DB_DATA_NODE")

NO_OF_SAMPLES = 100
IMPORT_MATRIX = [
    {"collection": "airline", "prefix": "inventory.airline"},
    {"collection": "airport", "prefix": "inventory.airport"},
    {"collection": "hotel", "prefix": "inventory.hotel"},
    {"collection": "landmark", "prefix": "inventory.landmark"},
    {"collection": "route", "prefix": "inventory.route"},
]


def check_collection_in_scope(data_host, bucket, username, password, scope, collection):
    """Check if the collection exists in the specified scope"""
    URL = f"https://{data_host}:18091/pools/default/buckets/{bucket}/scopes"
    basic = HTTPBasicAuth(username, password)
    response = requests.get(URL, auth=basic, verify=False)
    if response:
        # print(f"{response.status_code=}")
        result = response.json()
        # print(f"{result=}")

        # Parse required scope
        for scope_data in result["scopes"]:
            if scope_data["name"] == scope:
                # Parse collections in scope
                col_data = scope_data["collections"]
                collections = [c["name"] for c in col_data]
                # print(f"{collections=})
                break

    return collection in collections


def create_collection(cluster, bucket, scope, collection):
    """Create the collection on the cluster using the SDK"""
    try:
        colSpec = CollectionSpec(collection, scope_name=scope)
        bkt = cluster.bucket(bucket)
        bkt.collections().create_collection(colSpec)
    except Exception as e:
        print(f"Error while creating {collection=}: {e}")


# Check Environment Variables
# print(
#     f"{endpoint=} {username=} {password=} {bucket_name=} {scope_name=} {data_directory=} {data_host=}"
# )

# Connect options - authentication
auth = PasswordAuthenticator(username, password)

# Connect options - global timeout opts
timeout_opts = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=30))

# get a reference to our cluster
cluster = Cluster(
    f"couchbases://{endpoint}",
    ClusterOptions(auth, timeout_options=timeout_opts),
)

# Wait until the cluster is ready for use.
cluster.wait_until_ready(timedelta(seconds=5))

# get a reference to our bucket
cb = cluster.bucket(bucket_name)

print("Importing Data")
for data_import in IMPORT_MATRIX:

    collection = data_import["collection"]

    print(f"Importing Data into Collection {collection}")
    # TODO: Leave check & catch exception?
    if not check_collection_in_scope(
        data_host, bucket_name, username, password, scope_name, collection
    ):
        create_collection(cluster, bucket_name, scope_name, collection)
        time.sleep(5)

    cb_coll = cb.scope(scope_name).collection(collection)

    # Get list of docs to import matching the prefix specified
    all_files = list(
        pathlib.Path(data_directory).glob(f"{data_import['prefix']}.*.json")
    )

    # Pick random sample of docs
    random.seed(1000)
    files = random.sample(all_files, NO_OF_SAMPLES)

    # Lots of examples are using airline_10. So add it even if it is not there in the sampling
    if collection == "airline":
        files.append(
            pathlib.Path(f"{data_directory}/inventory.airline.airline_10.json")
        ),
        files.append(
            pathlib.Path(f"{data_directory}/inventory.airline.airline_5209.json")
        )

    docs_to_load = {}
    for f in tqdm(files):
        with open(f) as json_data:
            doc = json.load(json_data)
            key = f"{doc['type']}_{doc['id']}"
            docs_to_load[key] = doc
            # Individual document import
            # try:
            #     res = cb_coll.upsert(key, doc)
            # except Exception as e:
            #     print(f"Exception while inserting document, {e}")

    # Insert Documents in Bulk
    # print(f"{len(files)=} {len(docs_to_load)=}")
    try:
        res = cb_coll.upsert_multi(docs_to_load)
    except Exception as e:
        print(f"Exception while inserting document, {e}")

    # Create Primary Index
    try:
        query = f"CREATE PRIMARY INDEX ON `{bucket_name}`.{scope_name}.{collection}"
        cluster.query(query).execute()
    except Exception as e:
        print(f"Exception while creating primary index: {e}")

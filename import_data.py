import json
import multiprocessing
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
is_parallel = os.getenv("IS_PARALLEL", True)

def check_collection_in_scope(data_host, bucket, username, password, scope, collection):
    """Check if the collection exists in the specified scope"""
    URL = f"http://{data_host}:8091/pools/default/buckets/{bucket}/scopes"
    basic = HTTPBasicAuth(username, password)
    response = requests.get(URL, auth=basic, verify=False)
    if response:
        #print(f"{response.status_code=}")
        result = response.json()
        # print(f"{result=}")

        collections = []
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

    coll_manager = cluster.bucket(bucket).collections()
    try:
        coll_manager.create_scope(scope)
    except:
        pass

    """Create the collection on the cluster using the SDK"""
    try:
        colSpec = CollectionSpec(collection, scope_name=scope)
        coll_manager.create_collection(colSpec)
    except Exception as e:
        print(f"Error while creating {collection=}: {e}")



def import_into_collection(collection, docs_to_load):
    print("  Importing data into collection,",collection, len(docs_to_load[collection]))

    # Check Environment Variables
    #print(
    #    f"{endpoint=} {username=} {bucket_name=} {scope_name=} {data_directory=} {data_host=}"
    #)

    # Connect options - authentication
    auth = PasswordAuthenticator(username, password)

    # Connect options - global timeout opts
    timeout_opts = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=30))

    # get a reference to our cluster
    #print(f"Connecting to cluster: couchbase://{endpoint}")
    cluster = Cluster(
        f"couchbase://{endpoint}",
        ClusterOptions(auth)
    )

    # Wait until the cluster is ready for use.
    cluster.wait_until_ready(timedelta(seconds=5))
    #print("Connecting to cluster: done")

    # get a reference to our bucket
    cb = cluster.bucket(bucket_name)

    # TODO: Leave check & catch exception?
    if not check_collection_in_scope(
        data_host, bucket_name, username, password, scope_name, collection
    ):
        create_collection(cluster, bucket_name, scope_name, collection)
        time.sleep(5)

    cb_coll = cb.scope(scope_name).collection(collection)

    try:
        res = cb_coll.upsert_multi(docs_to_load[collection])
    except Exception as e:
        print(f"Exception while inserting document, {e}")

    # Create Primary Index
    try:
        query = f"CREATE PRIMARY INDEX ON `{bucket_name}`.{scope_name}.{collection}"
        cluster.query(query).execute()
    except Exception as e:
        print(f"Exception while creating primary index: {e}")
        
        
if __name__ == '__main__':


    start = time.perf_counter()

    print("Importing sample data into couchbase ...", endpoint, bucket_name, scope_name)
    # Get list of docs to import matching the prefix specified
    all_files = list(
        pathlib.Path(data_directory).glob("*.json")
    )

    print("Number of the json document files found: ", len(all_files))

    tables = {}
    docs_to_load = {}
    for f in tqdm(all_files):
        with open(f) as json_data:
            doc = json.load(json_data)
            try:
                doc_id = doc["id"]
                try:
                    doc_type = doc["type"]
                except KeyError:
                    doc_type = "_default"
                    print("No type specified, using default type: ", doc_type)
                tables[doc_type] = doc_type
                key = f"{doc_type}_{doc_id}"
                #print("key: ", key)
                try:
                    if not docs_to_load[doc_type]:
                        docs_to_load[doc_type] = {}
                except KeyError:
                    docs_to_load[doc_type] = {}
                docs_to_load[doc_type][key] = doc
            except Exception as e:
                pass

    print(f"Number of types/tables/collections found: {len(tables)}")
    for cname in tables.keys():
        print(f" {cname}")

    print("Importing data into collections, parallel: ", is_parallel)
        
    if is_parallel:
        # Insert Documents in bulk and parallel
        print("Parallel...")
        processes = []
        for collection in tables:
            p = multiprocessing.Process(target=import_into_collection, args=(collection, docs_to_load,))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()
    else:
        # Insert Documents in bulk
        print("Sequential...")
        for collection in tables:
            import_into_collection(collection, docs_to_load)

    end = time.perf_counter()
    print(f'Finished in {round(end-start, 2)} second(s)') 
        

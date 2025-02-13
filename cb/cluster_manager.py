from dotenv import load_dotenv
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from datetime import timedelta
import os 
import requests
from cb.index_manager import IndexManager as index_manager


class ClusterManager:

    load_dotenv()

    _cluster = None

    cb_host = os.getenv("CB_HOST")
    cb_user = os.getenv("CB_USERNAME")
    cb_pass = os.getenv("CB_PASSWORD")
    cb_bucket = os.getenv("CB_SCOPE") or "_default"


    @classmethod
    def disconnect(cls):
        if cls._cluster:
            cls._cluster.close()
            cls._cluster = None


    @classmethod
    def cluster(cls):
        if cls._cluster:
            return cls._cluster

        try:
            # Couchbase connection
            auth = PasswordAuthenticator(cls.cb_user, cls.cb_pass)
            cls._cluster = Cluster(f'couchbase://{cls.cb_host}', ClusterOptions(auth))
            cls._cluster.wait_until_ready(timedelta(seconds=5))
            print(f"Couchbase {cls.cb_host} connected.")
            return cls._cluster
        except Exception as e:
            print(f"Failed to connect to couchbase: {cls.cb_host}, {e}")
            raise

        
    @classmethod
    def import_fts_index(cls, index_name, index_json):
        url = f"http://{cls.cb_host}:8094/api/index/{index_name}"

        response = requests.put(url, auth=(cls.cb_user, cls.cb_pass), json=index_json)
        
        if response.status_code == 400:
            error = response.json()

        if not response.ok:
            raise RuntimeError(f"{response.status_code} {error}")
        

    @classmethod
    def create_vector_index(cls, index_name, bucket, scope, collection, info):
        index_json = index_manager.create_index_json(bucket, scope, collection, info)
        cls.import_fts_index(index_name, index_json)
        
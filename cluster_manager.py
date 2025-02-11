from dotenv import load_dotenv
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from datetime import timedelta
import os 

class ClusterManager:

    _cluster = None

    load_dotenv()

    @classmethod
    def disconnect(cls):
        if cls._cluster:
            cls._cluster.close()
            cls._cluster = None
            print(cls._cluster)

    @classmethod
    def cluster(cls):
        if cls._cluster:
            return cls._cluster

        cb_host = os.getenv("CB_HOST")

        try:
            # Couchbase connection
            auth = PasswordAuthenticator(os.getenv("CB_USERNAME"), os.getenv("CB_PASSWORD"))
            cls._cluster = Cluster(f'couchbase://{cb_host}', ClusterOptions(auth))
            cls._cluster.wait_until_ready(timedelta(seconds=5))
            print("Couchbase setup complete")
            return cls._cluster
        except Exception as e:
            print(f"Failed to connect to couchbase: {cb_host}, {e}")

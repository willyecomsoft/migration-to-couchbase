from cluster_manager import ClusterManager as cm
from dotenv import load_dotenv
import os 

load_dotenv()

def remove_db_connection():
  cm.disconnect()

def connect_to_couchbase(bucket):
  return cm.cluster().bucket(bucket)


def list_couchbase_collection(bucket):
  result = []

  scopes = connect_to_couchbase(bucket).collections().get_all_scopes()
  for scope in scopes:
    collections = scope.collections
    for collection in collections:
      result.append(f'{scope.name}.{collection.name}')

  return result


def connect_to_couchbase_collection(bucket, collection_name, scope_name=None):
  
  scope_name = scope_name or os.getenv("CB_SCOPE") or "_default" 
  collections = list_couchbase_collection(bucket)

  if f'{scope_name}.{collection_name}' in collections:
    return connect_to_couchbase(bucket).scope(scope_name).collection(collection_name)
  else:
    print(f"Collection {collection_name} does not exist in db")

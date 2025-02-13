from cb.cluster_manager import ClusterManager as cluster_manager
from cb.collection_manager import CollectionManager as collection_manager
from cb.index_manager import IndexManager as index_manager

def remove_db_connection():
  cluster_manager.disconnect()


def connect_to_couchbase(bucket):
  return cluster_manager.cluster().bucket(bucket)

        
def list_couchbase_collection(bucket):
   scope_collection_map = collection_manager.get_scope_collection_map(bucket)

   return [
      f'{scope}.{collection}' 
      for scope, collections in scope_collection_map.items() 
      for collection in collections
    ]


def connect_to_couchbase_collection(bucket, collection_name, scope_name=None):
  
  scope_name = scope_name or cluster_manager.cb_bucket 

  if collection_manager.collection_check(bucket, scope_name, collection_name).has_collection:
    return connect_to_couchbase(bucket).scope(scope_name).collection(collection_name)
  else:
    print(f"Collection {collection_name} does not exist in db")


def create_DSM_chunk_collection(
        bucket: str,
        collection_name: str,
        scope_name=None
):
  scope_name = scope_name or cluster_manager.cb_bucket 

  collection_manager.create_collection_with_scope(bucket, scope_name, collection_name)

  index = {
        'name': "Embedding_2",
        'dims': 1536,
        'similarity': 'l2_norm',
    }

  cluster_manager.create_vector_index("dsm_vector", bucket, scope_name , collection_name, index)


from typing import Any
from cb.cluster_manager import ClusterManager as cm
from collections import namedtuple

class CollectionManager:

    @classmethod
    def get_scope_collection_map(cls, bucket): 
        scope_collection_map: dict[str, Any] = {}
        
        # Get a list of all scopes in the bucket
        scopes = cm.cluster().bucket(bucket).collections().get_all_scopes()
        for scope in scopes:
            scope_collection_map[scope.name] = []
            
            # Get a list of all the collections in the scope
            for collection in scope.collections:
                scope_collection_map[scope.name].append(collection.name)

        return scope_collection_map
    

    @classmethod
    def collection_check(cls, bucket, scope, collection):
        CheckResult = namedtuple("CheckResult", ["has_scope", "has_collection"])

        scope_collection = cls.get_scope_collection_map(bucket).get(scope, None)
        if scope_collection is None:
            return CheckResult(False, False)
        else:
            return CheckResult(True, collection in scope_collection)
        
    
    @classmethod
    def create_collection_with_scope(cls, bucket, scope, collection):
        collection_manager = cm.cluster().bucket(bucket).collections()

        CheckResult = cls.collection_check(bucket, scope, collection)
        if not CheckResult.has_scope:
            collection_manager.create_scope(scope)
            collection_manager.create_collection(scope, collection)
        elif not CheckResult.has_collection:
            collection_manager.create_collection(scope, collection)
import requests
from collections import namedtuple
from cb.cluster_manager import ClusterManager as cm

class CollectionManager:        

    @classmethod
    def get_scope_collection_map(cls, bucket): 
        url = f"http://{cm.cb_host}:8091/pools/default/buckets/{bucket}/scopes"

        response = requests.get(url, auth=(cm.cb_user, cm.cb_pass))

        if response.ok:
            return {
                scope["name"]: [
                    collection["name"] for collection in scope["collections"]
                ] for scope in response.json()["scopes"]
            }
        else:
            print(f"{response.status_code}, {url}")


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

        CheckResult = cls.collection_check(bucket, scope, collection)
        if not CheckResult.has_scope:
            cls.create_scope(bucket, scope)
            cls.create_collection(bucket, scope, collection)
        elif not CheckResult.has_collection:
            cls.create_collection(bucket, scope, collection)


    @classmethod
    def create_scope(cls, bucket, scope):
        url = f"http://{cm.cb_host}:8091/pools/default/buckets/{bucket}/scopes"

        data = {
            'name': scope
        }

        response = requests.post(url, auth=(cm.cb_user, cm.cb_pass), data=data)

        error = None
        if response.status_code == 400:
            error = response.json()

        if not response.ok:
            raise RuntimeError(f"{response.status_code} {error}, Failed to create scope: {scope} in bucket: {bucket}")
        

    @classmethod
    def create_collection(cls, bucket, scope, collection):
        url = f"http://{cm.cb_host}:8091/pools/default/buckets/{bucket}/scopes/{scope}/collections"

        data = {
            'name': collection
        }

        response = requests.post(url, auth=(cm.cb_user, cm.cb_pass), data=data)

        error = None
        if response.status_code == 400:
            error = response.json()

        if not response.ok:
            raise RuntimeError(f"{response.status_code} {error}, Failed to create collection: {collection} in bucket: {bucket}")
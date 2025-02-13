# migration-to-couchbase


**create .env from .env.example**
```
CB_HOST=localhost
CB_USERNAME=Administrator
CB_PASSWORD=couchbase
CB_SCOPE=_default
```

## Note
- couchbase connection 由sdk自動管理


## main 
**dsm_couchbase_utils.py**


## others
|檔案|說明|
|--|--|
|cluster_manager|管理連線設定, 建立index|
|collection_manager|管理collection|
|collection_manager_rest|管理collection - rest api 版本|
|index_manager|產生index template json|
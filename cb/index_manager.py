class IndexManager:


    @classmethod
    def create_index_json(cls, bucket, scope, collection, info):
        mapping = cls.from_mapping_template(scope, collection, info)
        return cls.from_index_template(bucket, mapping)


    @classmethod
    def from_index_template(cls, bucket, mapping):
        return {
            "type": "fulltext-index",
            "params": {
            "doc_config": {
            "docid_prefix_delim": "",
            "docid_regexp": "",
            "mode": "scope.collection.type_field",
            "type_field": "type"
            },
            "mapping": mapping,
            "store": {
            "indexType": "scorch",
            "segmentVersion": 16
            }
            },
            "sourceType": "gocbcore",
            "sourceName": bucket,
            "sourceParams": {},
            "planParams": {
            "maxPartitionsPerPIndex": 1024,
            "indexPartitions": 1,
            "numReplicas": 0
            }
        }
    
    
    @classmethod
    def from_mapping_template(cls, scope, collection, info):
        return {
            "default_analyzer": "standard",
            "default_datetime_parser": "dateTimeOptional",
            "default_field": "_all",
            "default_mapping": {
                "dynamic": False,
                "enabled": False
            },
            "default_type": "_default",
            "docvalues_dynamic": False,
            "index_dynamic": False,
            "store_dynamic": False,
            "type_field": "_type",
            "types": { 
                f"{scope}.{collection}": {
                    "dynamic": True,
                    "enabled": True,
                    "properties": cls.from_vector_field_template(info)
                }
            }
        }
    

    @classmethod
    def from_vector_field_template(cls, info):
        template = {
            "dims": 1536,
            "index": True,
            "name": "Embedding",
            "similarity": "l2_norm",
            "type": "vector",
            "vector_index_optimized_for": "recall"
        }

        return {
            info.get("name", "Embedding"): {
                "enabled": True,
                "dynamic": False,
                "fields": [{**template, **info}]
            }
        }
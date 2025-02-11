#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  6 15:58:37 2024

@author: smgai
"""
from pymilvus import (
    connections, FieldSchema, CollectionSchema, DataType, Collection,
    utility
)


def remove_db_connection():
    connections.list_connections()
    return [connections.remove_connection(_conn_id) for (_conn_id, _grpc) in connections.list_connections()]


def connect_to_milvusdb(database, db_host, db_port, db_user, db_password):
    """ connect to milvus db """
    
    # initial setting: empty all db connection
    remove_db_connection()
    
    conn = connections.connect(
        db_name=database,
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password
    )
    return conn


def list_milvus_collection(database, db_host, db_port, db_user, db_password):
    
    # initial setting: empty all db connection
    remove_db_connection()
    
    connect_to_milvusdb(database, db_host, db_port, db_user, db_password)

    collection_lst = utility.list_collections()
    connections.disconnect(database)
    return collection_lst
    


def connect_to_milvus_collection(database, db_host, db_port, db_user, db_password, collection_name):
    
    # initial setting: empty all db connection
    remove_db_connection()
    
    connect_to_milvusdb(database, db_host, db_port, db_user, db_password)
    if utility.has_collection(collection_name):
        collection = Collection(name=collection_name)
    else:
        print(f"Collection {collection_name} does not exist in db")
    
    return collection


def create_DSM_chunk_collection(
        database: str,
        db_host: str,
        db_port: str,
        db_user: str,
        db_password: str,
        collection_name: str,
        description: str
):
    # initial setting: empty all db connection
    remove_db_connection()

    connect_to_milvusdb(database, db_host, db_port, db_user, db_password)
    
    fields = [
        FieldSchema(name='pk', dtype=DataType.INT64, is_primary=True, auto_id=True), # 45xxxx336xxxxxx70x
        FieldSchema(name='DocumentId', dtype=DataType.VARCHAR, max_length=200), # G-xx-MIXED_xxxxxxxxxxxxx-xxxx-Ver.0.1_P3.pdf
        FieldSchema(name='DocPath', dtype=DataType.VARCHAR, max_length=200), # G-xx-MIXED_xxxxxxxxxxxxx-xxxx-Ver.0.1_P3.pdf
        FieldSchema(name='DocId', dtype=DataType.VARCHAR, max_length=50), # 00xxxxxx-7xxx-4288-bfxx-65xxxxxxb60a
        FieldSchema(name='DocType', dtype=DataType.VARCHAR, max_length=30), # G-03
        FieldSchema(name='ChunkId', dtype=DataType.INT16), # 1
        FieldSchema(name='SectionName', dtype=DataType.VARCHAR, max_length=200), # Revision History
        FieldSchema(name='SectionChunkId', dtype=DataType.INT8), # 1
        FieldSchema(name='SectionNo', dtype=DataType.VARCHAR, max_length=200), # 4.5.1
        FieldSchema(name='Content', dtype=DataType.VARCHAR, max_length=65_535),
        FieldSchema(name='Embedding', dtype=DataType.FLOAT_VECTOR, dim=1536)
    ]
    schema = CollectionSchema(
        fields=fields,
        description=description,
        enably_dynamic_field=True    
    )
    collection = Collection(
        name=collection_name,
        schema=schema,
        # using='default',
        shards_num=2
    )
    
    # builds indexes on the vector field
    index_hnsw = {
        'index_type': 'HNSW',
        'metric_type': 'L2',
        'params': {'M': 16, 'efConstruction': 200}
    }
    collection.create_index('Embedding', index_hnsw)
    collection.load()
    return 
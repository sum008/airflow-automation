from pymongo import MongoClient

from bytecodeairflow.pipeline_meta_fetcher.secrets_loader import load_credentials_aws
import logging

logger= logging.getLogger(__name__)

class MongodbConnector:
    """
        To fetch mongodb collection and documents
    """

    def __init__(self, secret_name: str, region_name: str):
        """
        Initialize the connection credentials to connect with MongoDB
        Args:
            secret_name: The Name of the secret where the mongo credentials are stored
            region_name: The region name where the secret is store
            collection_name: Name of the collection from which need to pull data
        """
        self.credentials = load_credentials_aws(secret_name, region_name)
        self.user = self.credentials['user']
        self.password = self.credentials['password']
        self.host = self.credentials['host']
        self.port = self.credentials['port']
        self.options = self.credentials['options']
        self.global_pem_file_path = self.credentials['pem_file_path']
        self.database = self.credentials["common_config_database"]

        self.mongo_connection = self.__connect_to_mongodb()

    def __connect_to_mongodb(self):
        """
        Return Mongo client after successfully connecting with the database
        """
        connection_url = f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/{self.options}={self.global_pem_file_path}"
        return MongoClient(connection_url)

    def insert_metadata(self, data_to_insert, collection):
        return self.mongo_connection[self.database][collection].insert_one(data_to_insert)

    def update_metadata(self, *args, **kwargs):
        pass

    def fetch_metadata(self, collection, pipeline_id):
        return self.mongo_connection[self.database][collection].find_one(filter={"_id": pipeline_id})
    
    def delete_metadata(self, collection, pipeline_id):
        return self.mongo_connection[self.database][collection].delete_one({'_id': pipeline_id})
    
    def replace_metadata(self, collection, pipeline_id, new_metadata):
        return self.mongo_connection[self.database][collection].replace_one({"_id": pipeline_id}, new_metadata)

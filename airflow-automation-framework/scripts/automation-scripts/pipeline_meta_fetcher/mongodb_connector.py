from pymongo import MongoClient

from bytecodeairflow.common_helper.helper import load_config
from bytecodeairflow.pipeline_meta_fetcher.connector_base import ConnectorBase
from airflow.models import Variable

class MongodbConnector(ConnectorBase):
    def __init__(self):
        env = Variable.get('env')
        self.mongo_connection = self.connect_to_mongodb(connection_url=load_config(package_name="bytecodeairflow",filename="configurations/db_connections.json", extension="json")[env]["connection_url"])

    @staticmethod
    def connect_to_mongodb(connection_url):
        return MongoClient(connection_url)

    def insert_metadata(self, data_to_insert, collection, document):
        return self.mongo_connection[collection][document].insert_one(data_to_insert)

    def update_metadata(self, *args, **kwargs):
        pass

    def fetch_metadata(self, collection, document, pipeline_id):
        return self.mongo_connection[collection][document].find_one(filter={"_id": pipeline_id})
    
    def delete_metadata(self, collection, document, pipeline_id):
        return self.mongo_connection[collection][document].delete_one({'_id': pipeline_id})
    
    def replace_metadata(self, collection, document, pipeline_id, new_metadata):
        return self.mongo_connection[collection][document].replace_one({"_id": pipeline_id}, new_metadata)

import json

import pymongo


class MongoClient:

    def get_json_details(self, config_file):
        with open(config_file, "r") as infile:
            data = json.load(infile)

        return data.get("mongo_server", {})

    # def mongo_client(self):

    def __init__(self, config_file):
        mongo_data = self.get_json_details(config_file=config_file)
        self.username = mongo_data.get("username")
        self.password = mongo_data.get("password")
        self.host = mongo_data.get("host")
        self.port = mongo_data.get("port")
        self.database_name = mongo_data.get("database_name")
        self.collection_name = mongo_data.get("collection_name")

        if self.username:
            self.uri = mongo_data.get("uri").format(
                username=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
                database_name=self.database_name,
            )

        else:
            self.uri = mongo_data.get("uri_without_user").format(
                host=self.host,
                port=self.port,
                database_name=self.database_name
            )
        self.client = pymongo.MongoClient(self.uri)

    def insert_many(self, data):

        db = self.client[self.database_name]

        # Select the collection
        collection = db[self.collection_name]

        return collection.insert_many(data)

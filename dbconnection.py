#dbconnection.py


from pymongo import MongoClient

class MongoDBConnection:
    def __init__(self):
        self.host = None
        self.port = None
        self.username = None
        self.password = None
        self.connection_string = None
        self.client = None
        self.db = None

    def create_connection(self):
        cluster = input("please enter yes if you are using atlas mongoDB cluster: ")
        if cluster.lower() in ["yes", "y", "1"]:
            self.connection_string = input("please enter mongodb cluster url: ")
        else:
            self.host = input("Enter the database host (default: localhost): ") or "localhost"
            self.port = input("Enter the database port (default: 27017): ") or "27017"
            self.username = input("Enter the database username (leave blank if not applicable): ")
            self.password = input("Enter the database password (leave blank if not applicable): ")
            if self.username and self.password:
                self.connection_string = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/"
            else:
                self.connection_string = f"mongodb://{self.host}:{self.port}/"
        try:
            self.client = MongoClient(self.connection_string)
            print("MongoDB connection created successfully.")
        except Exception as e:
            print(f"Failed to create MongoDB connection: {e}")
            self.client = None

    def check_and_create_db(self, db_name, collection_names_list):
        if self.client is None:
            raise Exception("MongoDB client is not initialized.")
        if db_name in self.client.list_database_names():
            self.db = self.client[db_name]
            print(f"Database '{db_name}' already exists.")
        else:
            self.db = self.client[db_name]
            print(f"Database '{db_name}' created successfully.")

        existing_collections = self.db.list_collection_names()
        for collection_name in collection_names_list:
            if collection_name not in existing_collections:
                self.db.create_collection(collection_name)
                print(f"Collection '{collection_name}' created successfully.")
            else:
                print(f"Collection '{collection_name}' already exists.")

    def setup_database_and_collections(self, collection_names_list):
        clientname = input("Enter the clientname/databasename: ")
        self.create_connection()
        self.check_and_create_db(clientname, collection_names_list)
        collections = {name: self.db[name] for name in collection_names_list}
        return collections, clientname, self.connection_string


#  code to be used in main function
#collection_names_list = ["collection1", "collection2", "collection3"]
#mongo_conn = MongoDBConnection()
#collections = mongo_conn.setup_database_and_collections(collection_names_list)
#print(collections)
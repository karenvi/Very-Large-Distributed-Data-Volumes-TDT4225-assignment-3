from pprint import pprint 
from DbConnector import DbConnector


class Program:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
    
    def insert_user(self, id, has_labels):
        doc = {
            '_id': id,
            'has_labels': has_labels
        }
        self.db.User.insert_one(doc)
        
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

        
    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)
         


def main():
    program = None
    try:
        program = Program()
        program.insert_user("000", True)
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()

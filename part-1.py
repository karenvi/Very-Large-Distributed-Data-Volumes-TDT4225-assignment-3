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
        self.db.User.replace_one({'_id':id}, doc, upsert=True) # if user exists update with doc, else insert doc
        #self.db.User.insert_one(doc)
        
    def insert_activity(self, user_id, transportation_mode, start_date_time, end_date_time):       
        doc = {
        "user_id": user_id, # because _id is not set, mongodb will set _id to unique objectID
        "transportation_mode": transportation_mode,
        "start_date_time": start_date_time,
        "end_date_time": end_date_time
        }
        self.db.Activity.replace_one({'user_id':user_id}, doc, upsert=True) # burde vel egt heller query _id siden den er unik

    # note til austad, user 175 har ikke så mange plt filer og den har labels så fin for testing
    
    def insert_trackpoint(self, currentID):
        doc = {
        "activity_id": None, # none er bare placeholder atm
        "lat": None,
        "lon": None,
        "altitude": None,
        "date_time": None
        }
        self.db.TrackPoint.replace_one({'_id':currentID}, doc, upsert=True)
        
       
    def fetch_last_insert_id(self, collection):
        id = self.db[collection].find().sort("_id",-1).limit(1)[0]["_id"] 
        # sort("_id", -1) gir nyeste. 1 hadde gitt eldste
        # Se https://stackoverflow.com/questions/53581201/mongodb-console-getting-the-id-of-a-cursor-object for forklaring på id
        print("Latest inserted document in", collection, "has id", id)
        return id


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
        program.insert_user("001", True)
        program.fetch_last_insert_id("Activity")
        #program.insert_user("000", False)
        program.insert_activity("000", None, "2009-10-11, 14:04:30", "2009-10-11, 14:04:35")
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()

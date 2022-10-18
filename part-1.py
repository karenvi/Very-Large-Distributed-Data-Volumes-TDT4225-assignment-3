from pprint import pprint 
from DbConnector import DbConnector
import pandas as pd
import os
from decouple import config
from bson.objectid import ObjectId


class Program:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
    
    def insert_user(self, id, has_labels, activities):
        doc = {
            '_id': id,
            'has_labels': has_labels,
            'activities': activities
        }
        self.db.User.replace_one({'_id':id}, doc, upsert=True) # if user exists update with doc, else insert doc
        #self.db.User.insert_one(doc)
        
    def insert_activity(self, user_id, transportation_mode, start_date_time, end_date_time):       
        doc = {
        # because _id is not set, mongodb will set _id to unique objectID
        "user_id": user_id, 
        "transportation_mode": transportation_mode,
        "start_date_time": start_date_time,
        "end_date_time": end_date_time
        }
        self.db.Activity.replace_one({'user_id':user_id}, doc, upsert=True) # burde vel egt heller query _id siden den er unik
    
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
        # sort("_id", -1) gir nyeste. 1 hadde gitt eldste
        # Se https://stackoverflow.com/questions/53581201/mongodb-console-getting-the-id-of-a-cursor-object for forklaring på id
        id = self.db[collection].find().sort("_id",-1).limit(1)[0]["_id"] 
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
        
    def insert_dataset(self):
        dataset_path = config('DATASET_PATH')
        # Read labeled_ids.txt file
        labeled_ids = pd.read_csv(
            f'{dataset_path}/labeled_ids.txt', delim_whitespace=True, header=None, dtype=str)

        subfolders = os.listdir(f'{dataset_path}/Data')
        for i, user in enumerate(subfolders):
            print(f'[{i + 1}/{len(subfolders)}] Inserting User: {user}')

            # Check if user is in labeled_ids.txt
            if user in labeled_ids.values:
                user_has_labels = True
            else:
                user_has_labels = False

            user_dir = f'{dataset_path}/Data/{user}/{"Trajectory"}'

            activities = []

            # Iterate through all activities for a specific user
            for activity in os.listdir(user_dir):
                plt_path = f'{user_dir}/{activity}'
                file = pd.read_csv(
                    plt_path,
                    skiprows=6,
                    header=None,
                    parse_dates=[[5, 6]],
                    infer_datetime_format=True
                )

                # Only insert activities with less than or equal 2500 trackpoints
                if (len(file.index) <= 2500):
                    # Rename columns for clarity and remove unused columns
                    file.rename(
                        inplace=True,
                        columns={
                            0: 'lat',
                            1: 'lon',
                            3: 'alt',
                            4: 'date_days',
                            '5_6': 'date_time'
                        })
                    file.drop(inplace=True, columns=[2])

                    # Fetch start and end time for the activity
                    start_date_time = pd.to_datetime(
                        file.head(1)['date_time'].values[0], format="%Y/%m/%d %H:%M:%S"
                    )
                    end_date_time = pd.to_datetime(
                        file.tail(1)['date_time'].values[0], format="%Y/%m/%d %H:%M:%S"
                    )

                    if user in labeled_ids.values:
                        # Read labels.txt file and rename columns for clarity
                        labels = pd.read_csv(
                            f'{os.path.dirname(user_dir)}/labels.txt',
                            delim_whitespace=True,
                            skiprows=1,
                            header=None,
                            parse_dates=[[0, 1], [2, 3]],
                            infer_datetime_format=True
                        )
                        labels.rename(
                            inplace=True,
                            columns={
                                '0_1': 'start_date_time',
                                '2_3': 'end_date_time',
                                4: 'transportation_mode'
                            }
                        )

                        # Match start time and end time in labels
                        matching_row = labels[(
                            (labels['start_date_time'] == start_date_time) & 
                            (labels['end_date_time'] == end_date_time)
                        )]

                        # Check if there is a match
                        if len(matching_row) > 0:
                            transportation_mode = matching_row['transportation_mode'].values[0]
                            activity_id = ObjectId()
                            activity = {
                                "_id": activity_id,
                                "transportation_mode": transportation_mode,
                                "start_date_time": start_date_time,
                                "end_date_time": end_date_time
                            }
                            activities.append(activity)
                            # self.insert_activity(
                            #     user, transportation_mode, start_date_time, end_date_time)
                            # activity_id = self.fetch_last_insert_id()
                            # file['activity_id'] = activity_id
                            # self.insert_track_points_batch(
                            #     list(file.itertuples(index=False, name=None)))
                        else:
                            # self.insert_activity(
                            #     user, 'NULL', start_date_time, end_date_time)
                            # activity_id = self.fetch_last_insert_id()
                            # file['activity_id'] = activity_id
                            # self.insert_track_points_batch(
                            #     list(file.itertuples(index=False, name=None)))
                            activity_id = ObjectId()
                            activity = {
                                "_id": activity_id,
                                "transportation_mode": "NULL",
                                "start_date_time": start_date_time,
                                "end_date_time": end_date_time
                            }
                            activities.append(activity)
                    else:
                        # self.insert_activity(
                        #     user, 'NULL', start_date_time, end_date_time)
                        # activity_id = self.fetch_last_insert_id()
                        # file['activity_id'] = activity_id
                        # self.insert_track_points_batch(
                        #     list(file.itertuples(index=False, name=None)))
                        activity_id = ObjectId()
                        activity = {
                            "_id": activity_id,
                            "transportation_mode": "NULL",
                            "start_date_time": start_date_time,
                            "end_date_time": end_date_time
                        }
                        activities.append(activity)
                self.insert_user(user, user_has_labels, activities)
         

def main():
    program = None
    try:
        program = Program()
        #program.insert_activity("000", None, "2009-10-11, 14:04:30", "2009-10-11, 14:04:35")
        #program.fetch_last_insert_id("Activity")
        
        program.insert_dataset()
        #program.drop_coll("User")
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
        
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()

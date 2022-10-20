from pprint import pprint
import re
from unittest import result 
from DbConnector import DbConnector
import pandas as pd
import os
from decouple import config
from bson.objectid import ObjectId
from tqdm import tqdm
from tabulate import tabulate


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
        # If user exists update with doc, else insert doc
        self.db.User.replace_one({'_id':id}, doc, upsert=True)

    def insert_trackpoints(self, trackpoints):
        self.db.TrackPoint.insert_many(trackpoints)

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
            f'{dataset_path}/labeled_ids.txt',
            delim_whitespace=True,
            header=None,
            dtype=str
        )

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
            for activity in tqdm(os.listdir(user_dir)):
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
                            file['activity_id'] = activity_id
                            trackpoints = file.to_dict(orient='records')
                            self.insert_trackpoints(trackpoints)
                        else:
                            activity_id = ObjectId()
                            activity = {
                                "_id": activity_id,
                                "transportation_mode": None,
                                "start_date_time": start_date_time,
                                "end_date_time": end_date_time
                            }
                            activities.append(activity)
                            file['activity_id'] = activity_id
                            trackpoints = file.to_dict(orient='records')
                            self.insert_trackpoints(trackpoints)
                    else:
                        activity_id = ObjectId()
                        activity = {
                            "_id": activity_id,
                            "transportation_mode": None,
                            "start_date_time": start_date_time,
                            "end_date_time": end_date_time
                        }
                        activities.append(activity)
                        file['activity_id'] = activity_id
                        trackpoints = file.to_dict(orient='records')
                        self.insert_trackpoints(trackpoints)
            self.insert_user(user, user_has_labels, activities)

    def task_1(self):
        print("TASK 1: Count the number of users, activities and trackpoints. \n")
        users = self.db["User"]
        usersTotal = users.count_documents(filter={})
        print("Number of users: ", usersTotal)

        activitiesTotal = users.aggregate([{"$unwind": "$activities"},{"$count": "activities"}]).next()
        print("Number of activities:", list(activitiesTotal.values()).pop())

        trackpointsTotal = self.db["TrackPoint"].count_documents(filter={})
        print("Number of trackpoints:", trackpointsTotal)


    def task_3(self):
        print("\n---\n\nTASK 3: Find the top 20 users with the highest number of activities \n")
        users = self.db["User"]
        top20 = users.aggregate([
            {"$project": {"Activities": {"$size":"$activities"}}},
            {"$sort": {"Activities": -1}}, # -1 means descending order
            {"$limit": 20}
        ])
        print(tabulate(top20, headers="keys"))

    
    def task_4(self):
        print("\n---\n\nTASK 4: Find all users who have taken a taxi \n")
        result = self.db.User.distinct("_id", {"activities.transportation_mode": "taxi"})
        print("Users who have taken taxi:")
        [print(i) for i in result]


def main():
    program = None
    try:
        program = Program()
        program.task_1()
        program.task_3()
        program.task_4()
        #program.insert_dataset()
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
        
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()

from pprint import pprint 
from DbConnector import DbConnector
import pandas as pd
import os
from decouple import config
from bson.objectid import ObjectId
from tqdm import tqdm

class Queries:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
    
    def task_9(self):
        print("\n---\n\nTASK 9: Find all users who have invalid activities, and the number of invalid activities per user \n")
        user_collection = self.db["User"]
        users = user_collection.find(no_cursor_timeout=True)

        users_with_invalid_activities = []

        data = dict()

        for user in users:
            trackpoints = []
            for activity in tqdm(user["activities"]):
                matching_trackpoints = self.db.TrackPoint.find({"activity_id" : ObjectId(activity["_id"])}, no_cursor_timeout=True)
                trackpoints.append(list(matching_trackpoints))
                matching_trackpoints.close()

            # Build a dictionary with user ID as key, containing trackpoints for the user
            data[user["_id"]] = trackpoints


        for user in data.items():
            num_invalid_activities = 0
            print(user[0])
            for trackpoints in user[1]:
                for i in range(0, len(trackpoints)-1):
                    previous_trackpoint = trackpoints[i]["date_time"]
                    current_trackpoint = trackpoints[i+1]["date_time"]

                    time_diff = (current_trackpoint - previous_trackpoint).total_seconds()

                    if (time_diff > 300.0):
                        # Convert from feet to meters
                        num_invalid_activities += 1

                        break

            if (num_invalid_activities != 0):
                users_with_invalid_activities.append({"user": user[0], "invalid_activities": num_invalid_activities})
        
        users.close()

        print("\nAll users who have invalid activities, and the number of invalid activities per user: \n")
        pprint(sorted(users_with_invalid_activities, key=lambda d: d['user']))


def main():
    queries = None
    try:
        queries = Queries()
        queries.task_9()
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
        
    finally:
        if queries:
            queries.connection.close_connection()


if __name__ == '__main__':
    main()
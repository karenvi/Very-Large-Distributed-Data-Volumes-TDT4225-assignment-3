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
    
    def task_8(self):
        print("\n---\n\nTASK 8: Find the top 20 users who have gained the most altitude meters \n")
        user_collection = self.db["User"]
        #trackpoint_collection = self.db["TrackPoint"]

        users = user_collection.find(no_cursor_timeout=True)
        #activities = list(user_collection.aggregate([{'$unwind': '$activities'}]))
        #trackpoints = trackpoint_collection.find()

        highest_altitude_users = []

        data = dict()

        for user in users:
            trackpoints = []
            for activity in tqdm(user["activities"]):
                trackpoints.append(list(self.db.TrackPoint.find({"activity_id" : ObjectId(activity["_id"])})))

            # Build a dictionary with user ID as key, containing trackpoints for the user
            data[user["_id"]] = trackpoints


        for user in data.items():
            gained_altitude = 0
            print(user[0])
            for trackpoints in user[1]:
                for i in range(0, len(trackpoints)-1):
                    previous_trackpoint = trackpoints[i]["alt"]
                    current_trackpoint = trackpoints[i+1]["alt"]

                    if (current_trackpoint != -777 and current_trackpoint > previous_trackpoint):
                        # Convert from feet to meters
                        gained_altitude += (current_trackpoint - previous_trackpoint) * 0.0003048

            highest_altitude_users.append({"user": user[0], "total_meters_gained": gained_altitude})
        
        users.close()

        print("\nThe top 20 users who have gained the most altitude meters: \n")
        pprint(sorted(highest_altitude_users, key=lambda d: d['total_meters_gained'], reverse=True)[:20])


def main():
    queries = None
    try:
        queries = Queries()
        queries.task_8()
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
        
    finally:
        if queries:
            queries.connection.close_connection()


if __name__ == '__main__':
    main()
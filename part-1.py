from pprint import pprint
from random import random 
from unittest import result 
from enum import unique
import operator
from pprint import pprint
from tracemalloc import start 
from DbConnector import DbConnector
import pandas as pd
import os
from decouple import config
from bson.objectid import ObjectId
import datetime
from tqdm import tqdm
from tabulate import tabulate
from haversine import haversine



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
        print("\n---\n\nTASK 1: Count the number of users, activities and trackpoints. \n")
        users = self.db["User"]
        usersTotal = users.count_documents(filter={})
        print("Number of users: ", usersTotal)

        activitiesTotal = users.aggregate([{"$unwind": "$activities"},{"$count": "activities"}]).next()
        print("Number of activities:", list(activitiesTotal.values()).pop())

        trackpointsTotal = self.db["TrackPoint"].count_documents(filter={})
        print("Number of trackpoints:", trackpointsTotal)
    
    def task_2(self):
        print("\n---\n\nTASK 2: Find average number of activities per user.\n")
        user_collection = self.db["User"]
        count_users = user_collection.count_documents(filter={})
        # Since the activities are in an array in the collection User we must use $unwind to deconstruct the array field
        count_activities = user_collection.aggregate([
            {"$unwind": "$activities"},
            {"$count": "activities"}]).next()
        print("The average number of activities per user is: " + str(count_activities["activities"] / count_users))

    def task_5(self):
        print("\n---\n\nTASK 5: Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels.\n")
        user_collection = self.db["User"]
        user_ids = list(user_collection.find({}))
        all_transport = []
        distinct_transport = []
        transport_modes_and_values = {}

        for user in user_ids:
            activities_documents = user["activities"]
            for activity in activities_documents:
                transportation_modes = activity["transportation_mode"]
                if (transportation_modes != None):
                    all_transport.append(transportation_modes)
        
        for transport in all_transport:
            if (transport not in distinct_transport):
                distinct_transport.append(transport)
        
        for transport in distinct_transport:
            transport_modes_and_values[transport] = 0
        
        for transport in all_transport:
            for transport_mode in distinct_transport:
                if (transport == transport_mode):
                    transport_modes_and_values[transport_mode] += 1
        
        for key, value in transport_modes_and_values.items():
            print(key + ": " + str(value))

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

    def task_7(self):
        print("\n---\n\nTASK 7: Find the total distance (in km) walked in 2008, by user with id=112 \n")
        activities = list(self.db.User.find({"_id": "112"}))[0]["activities"]
        
        # First filter through user 112's activities to only inlcude 2008 activities + activities with transportation mode "walk"
        filteredActivities = []
        for activity in activities:
            if (activity["transportation_mode"] == "walk" and datetime.datetime.strptime(str(activity["start_date_time"]), "%Y-%m-%d %H:%M:%S").year == 2008):
                filteredActivities.append(activity)

        # Then find all trackpoints for the filtered activities by matching the activity_id in the TrackPoint collection
        # with each activity's _id in filteredActivities 
        trackpoints = []
        for activity in tqdm(filteredActivities): # this will take some time; for testing behaviour reduce to e.g. filteredActivities[:3]
            tp = list(self.db.TrackPoint.find({"activity_id" : ObjectId(activity["_id"])})) 
            trackpoints.append(tp)
        
        #print(trackpoints) # uncomment to view nested structure of trackpoints list

        totalDistance = 0
        for i in range(0, len(trackpoints)-1): 
            # we currently need two for loops because the lat/lon values are nested at several levels in the trackpoints list
            for trackpoint in range(0, len(trackpoints[i])-1):
                fromLoc = (trackpoints[i][trackpoint]["lat"], trackpoints[i][trackpoint]["lon"])
                toLoc = (trackpoints[i][trackpoint+1]["lat"], trackpoints[i][trackpoint+1]["lon"])
                totalDistance += haversine(fromLoc, toLoc) 

        print("\n User with id=112 walked", round(totalDistance), 'km in 2008')
        
        
    def task_10(self):
        print("\n---\n\nTASK 10: Find the users who have tracked an activity in the Forbidden City of Beijing.\n")
        trackpoint_collection = self.db["TrackPoint"]
        user_collection = self.db["User"]
        # gte = greater than and lt = less than
        trackpoints = list(trackpoint_collection.find({
            "lat": {"$gte":39.915,"$lt":39.917},
            "lon": {"$gte":116.396,"$lt":116.398}
            }))

        unique_ids = []
        for i in range(0, len(trackpoints)):
            if trackpoints[i]["activity_id"] not in unique_ids:
                unique_ids.append(trackpoints[i]["activity_id"])

        users_in_beijing = list(user_collection.find({'$or': [{ 'activities._id': unique_ids[0] }, { 'activities._id': unique_ids[1] }, { 'activities._id': unique_ids[2] }, { 'activities._id': unique_ids[3] }, { 'activities._id': unique_ids[4] }, { 'activities._id': unique_ids[5] }]}))

        print("Users who have tracked an activity in the Forbidden City of Beijing:")
        for item in users_in_beijing:
            print(item["_id"])
            
    def task_6(self):
        print("\n---\n\nTASK 6a: Find the year with the most activities. \n")
        user_collection = self.db["User"]
        user_list = list(user_collection.aggregate([{
            '$unwind': '$activities'
            }]))
        all_years = []
        distinct_years = []
        count_activities_per_year = {}
        count_hours_per_year = {}

        # Basically doing the same as in task 5
        for user in user_list:
            actitivity_docs = user["activities"]
            end_year = str(actitivity_docs["end_date_time"])
            all_years.append(end_year.split("-")[0])

        for year in all_years:
            if (year not in distinct_years):
                distinct_years.append(year)
                count_activities_per_year[year] = 0
                count_hours_per_year[year] = 0
            for uniqueYear in distinct_years:
                if (year == uniqueYear):
                    count_activities_per_year[uniqueYear] += 1
        
        sorted_dict_year = sorted(count_activities_per_year.items(), key=operator.itemgetter(1), reverse=True)
        print("Most activities registered in " + sorted_dict_year[0][0] + " with a total of " + str(sorted_dict_year[0][1]) + " activities!")

        print("\nTASK 6b: Is this also the year with most recorded hours?\n")
        for user in user_list:
            actitivity_docs = user["activities"]
            start_date = actitivity_docs["start_date_time"]
            end_date = actitivity_docs["end_date_time"]
            only_year = str(start_date).split("-")[0]
            hours = (end_date - start_date).seconds
            count_hours_per_year[only_year] += round(hours / 3600)
        
        sorted_hours = sorted(count_hours_per_year.items(), key=operator.itemgetter(1), reverse=True)
        print("No, " + sorted_hours[0][0] + " is the year with the most recorded hours. It has " + str(sorted_hours[0][1]) + " hours recorded.")
            




def main():
    program = None
    try:
        program = Program()
        program.task_1()
        program.task_2()
        program.task_5()
        
        program.task_3()
        program.task_4()
        # program.task_7()
        #program.insert_dataset()
        # program.insert_dataset()
        # program.insert_dataset()
        program.task_10()
        # program.insert_dataset()
        program.task_6()
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
        
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()

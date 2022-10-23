from pprint import pprint 
from DbConnector import DbConnector
import pandas as pd
import os
from decouple import config
from bson.objectid import ObjectId
from tqdm import tqdm
from tabulate import tabulate
import operator
import datetime
from haversine import haversine

class Queries:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
    
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

    def task_7(self):
        print("\n---\n\nTASK 7: Find the total distance (in km) walked in 2008, by user with id=112 \n")
        activities = list(self.db.User.find({"_id": "112"}))[0]["activities"]
        
        # First filter through user 112's activities to only inlcude 2008 activities + activities with transportation mode "walk"
        filteredActivities = []
        for activity in activities:
            if (activity["transportation_mode"] == "walk" and datetime.datetime.strptime(str(activity["start_date_time"]), "%Y-%m-%d %H:%M:%S").year == 2008):
                filteredActivities.append(activity)

        # Create index to speed up read queries
        trackpointCollection = self.db["TrackPoint"]
        trackpointCollection.create_index([("activity_id", 1)])

        # Then find all trackpoints for the filtered activities by matching the activity_id in the TrackPoint collection
        # with each activity's _id in filteredActivities 
        matchedTrackpoints = []
        
        for activity in filteredActivities: 
            tp = list(trackpointCollection.find({"activity_id" : ObjectId(activity["_id"])})) 
            matchedTrackpoints.append(tp)
        
        #print(matchedTrackpoints) # uncomment to view nested structure of matchedTrackpoints list

        totalDistance = 0
        for i in range(0, len(matchedTrackpoints)-1): 
            # we currently need two for loops because the lat/lon values are nested at several levels in the matchedTrackpoints list
            for trackpoint in range(0, len(matchedTrackpoints[i])-1):
                fromLoc = (matchedTrackpoints[i][trackpoint]["lat"], matchedTrackpoints[i][trackpoint]["lon"])
                toLoc = (matchedTrackpoints[i][trackpoint+1]["lat"], matchedTrackpoints[i][trackpoint+1]["lon"])
                totalDistance += haversine(fromLoc, toLoc) 

        print("\nUser with id=112 walked", round(totalDistance), 'km in 2008')

    def task_8(self):
        print("\n---\n\nTASK 8: Find the top 20 users who have gained the most altitude meters \n")

        user_collection = self.db["User"]
        trackpoint_collection = self.db["TrackPoint"]

        # Create index to significantly speed up read queries
        trackpoint_collection.create_index([("activity_id", 1)])

        users = user_collection.find()
        highest_altitude_users = []

        data = dict()

        for user in users:
            trackpoints = []
            for activity in tqdm(user["activities"]):
                matching_trackpoints = trackpoint_collection.find({"activity_id" : ObjectId(activity["_id"])})
                trackpoints.append(list(matching_trackpoints))

            # Build a dictionary with user ID as key, containing trackpoints for the user
            data[user["_id"]] = trackpoints

        for user in data.items():
            gained_altitude = 0
            for trackpoints in user[1]:
                for i in range(0, len(trackpoints)-1):
                    previous_trackpoint = trackpoints[i]["alt"]
                    current_trackpoint = trackpoints[i+1]["alt"]

                    if (current_trackpoint != -777 and current_trackpoint > previous_trackpoint):
                        # Convert from feet to meters
                        gained_altitude += (current_trackpoint - previous_trackpoint) * 0.0003048

            highest_altitude_users.append({"user": user[0], "total_meters_gained": gained_altitude})

        print("\nThe top 20 users who have gained the most altitude meters: \n")
        pprint(sorted(highest_altitude_users, key=lambda d: d['total_meters_gained'], reverse=True)[:20])

def task_9(self):
    print("\n---\n\nTASK 9: Find all users who have invalid activities, and the number of invalid activities per user \n")
    user_collection = self.db["User"]
    trackpoint_collection = self.db["TrackPoint"]

    # Create index to significantly speed up read queries
    trackpoint_collection.create_index([("activity_id", 1)])

    users = user_collection.find()
    users_with_invalid_activities = []

    data = dict()

    for user in users:
        trackpoints = []
        for activity in tqdm(user["activities"]):
            matching_trackpoints = trackpoint_collection.find({"activity_id" : ObjectId(activity["_id"])})
            trackpoints.append(list(matching_trackpoints))

        # Build a dictionary with user ID as key, containing trackpoints for the user
        data[user["_id"]] = trackpoints

    for user in data.items():
        num_invalid_activities = 0
        for trackpoints in user[1]:
            for i in range(0, len(trackpoints)-1):
                previous_trackpoint = trackpoints[i]["date_time"]
                current_trackpoint = trackpoints[i+1]["date_time"]

                time_diff = (current_trackpoint - previous_trackpoint).total_seconds()

                # 5 minute deviation
                if (time_diff >= 300.0):
                    num_invalid_activities += 1
                    # Do not need to check the rest of the trackpoints
                    break

        if (num_invalid_activities != 0):
            users_with_invalid_activities.append({"user": user[0], "invalid_activities": num_invalid_activities})

    print("\nAll users who have invalid activities, and the number of invalid activities per user: \n")
    pprint(sorted(users_with_invalid_activities, key=lambda d: d['user']))
    
        
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
            
    
    
def task_11(self):
    print("\nTASK 11: Find all users who have registered transportation_mode and their most used transportation_mode\n")
    user_collection = self.db["User"]
    user_ids = list(user_collection.find({}))
    users_and_transport = []
    distinct_users = []
    distinct_transport = []
    transportChoiceOfUser = {}

    for user in user_ids:
        activities = user["activities"]
        for activity in activities:
            transportation_mode = activity["transportation_mode"]
            if (transportation_mode != None):
                users_and_transport.append([user["_id"], transportation_mode])

    for usersAndTransport in users_and_transport:
        if (usersAndTransport[0] not in distinct_users):
            distinct_users.append(usersAndTransport[0])
        if (usersAndTransport[1] not in distinct_transport):
            distinct_transport.append(usersAndTransport[1])
    print("User id | Most used transportation mode")
    for user in distinct_users:
        for transport in distinct_transport:
            transportChoiceOfUser[transport] = 0
            for usersAndTransport in users_and_transport:
                if (transport == usersAndTransport[1] and user == usersAndTransport[0]):
                    transportChoiceOfUser[transport] += 1
        sorted_transportChoiceOfUser = sorted(transportChoiceOfUser.items(), key=operator.itemgetter(1), reverse=True)
        # Problem description only asks for the user id and their most used mode
        print(user, sorted_transportChoiceOfUser[0][0])
        # print(user, sorted_transportChoiceOfUser[0][0], sorted_transportChoiceOfUser[0][1])
        


def main():
    queries = None
    try:
        queries = Queries()
        
        queries.task_1()
        queries.task_2()
        queries.task_3()
        queries.task_4()
        queries.task_5()
        queries.task_6()
        queries.task_7()
        queries.task_8()
        queries.task_9()
        queries.task_10()
        queries.task_11()
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
        
    finally:
        if queries:
            queries.connection.close_connection()


if __name__ == '__main__':
    main()
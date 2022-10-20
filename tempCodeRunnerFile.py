            fromLoc = (trackpoints[trackpoint]["lat"], trackpoints[trackpoint]["lon"])
            toLoc = (trackpoints[trackpoint + 1]["lat"], trackpoints[trackpoint + 1]["lon"])
            totalDistance += haversine(fromLoc, toLoc) 
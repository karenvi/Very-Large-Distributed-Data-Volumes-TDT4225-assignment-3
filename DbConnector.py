from pymongo import MongoClient, version
from decouple import config


class DbConnector:
    """
    Connects to the MongoDB server on the Ubuntu virtual machine.
    Connector needs HOST, USER and PASSWORD to connect.

    Example:
    HOST = "tdt4225-00.idi.ntnu.no" // Your server IP address/domain name
    USER = "testuser" // This is the user you created and added privileges for
    PASSWORD = "test123" // The password you set for said user
    """

    def __init__(self,
                 DATABASE=config('DATABASE'),
                 HOST=config('HOST'),
                 USER=config('MONGO_DB_USER'),
                 PASSWORD=config('PASSWORD'),
                 ENABLE_VM=config('ENABLE_VM', '0') == '1'):

        if ENABLE_VM:
            uri = "mongodb://%s:%s@%s/%s" % (USER, PASSWORD, HOST, DATABASE)
        else:
            uri = f"mongodb://localhost:27017/{DATABASE}"

        # Connect to the databases
        try:
            self.client = MongoClient(uri)
            self.db = self.client[DATABASE]
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

        # get database information
        print("You are connected to the database:", self.db.name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        # close the cursor
        # close the DB connection
        self.client.close()
        print("\n-----------------------------------------------")
        print("Connection to %s-db is closed" % self.db.name)

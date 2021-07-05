from pymongo import MongoClient
from numpy import median

# constants
SUCCESS = 0
ALREADY_EXISTS = 1


class DataBase:

    def __init__(self):
        self.mongo_client, self.db = self.connect_to_db()
        # clear the data base from irrelevant collections.
        self.clear_collections_from_db()
        # initialize the collections ( requests, impressions, clicks )
        self.db.create_collection("requests")
        self.db.create_collection("impressions")
        self.db.create_collection("clicks")

    def clear_collections_from_db(self):
        self.db["requests"].drop()
        self.db["impressions"].drop()
        self.db["clicks"].drop()

    def create_collection(self, col_name):
        if self.db.is_collection_exists(col_name):
            return False
        self.db.createCollection(col_name, autoIndexId=False)
        return True

    def is_collection_exists(self, col_name):
        collection_list = self.db.list_collection_names()
        if col_name in collection_list:
            return True
        else:
            return False

    def connect_to_db(self):
        mongo_client = MongoClient()
        db = mongo_client["MyDB"]
        return mongo_client, db

    def is_request_already_exists(self, user_id, session_id):
        if self.db["requests"].count_documents({"user_id": user_id, "session_id": session_id}) > 0:
            return True
        else:
            return False

    def is_impression_already_exists(self, session_id):
        if self.db["impressions"].count_documents({"session_id": session_id}) > 0:
            return True
        else:
            return False

    def is_click_already_exists(self, session_id):
        if self.db["clicks"].count_documents({"session_id": session_id}) > 0:
            return True
        else:
            return False

    def add_request(self, doc_json):
        if self.is_request_already_exists(doc_json["user_id"], doc_json["session_id"]):
            return ALREADY_EXISTS
        else:
            self.db["requests"].insert_one(doc_json)
            return SUCCESS

    def add_impression(self, doc_json):
        if self.is_impression_already_exists(doc_json["session_id"]):
            return ALREADY_EXISTS
        else:
            self.db["impressions"].insert_one(doc_json)
            return SUCCESS

    def add_click(self, doc_json):
        if self.is_click_already_exists(doc_json["session_id"]):
            return ALREADY_EXISTS
        else:
            self.db["clicks"].insert_one(doc_json)
            return SUCCESS

    # --------------------- Methods for /userStats route helper ---------------------------------
    def get_num_requests_by_user_id(self, user_id):
        query = {"user_id": user_id}
        return self.db["requests"].count_documents(query)

    def get_session_ids_by_user_id(self, user_id):
        query = {"user_id": user_id}
        projection = {"_id": 0, "session_id": 1}
        return self.db["requests"].find(query, projection)

    def get_number_of_impressions_by_session_id(self, session_id):
        query = {"session_id": session_id}
        return self.db["impressions"].count_documents(query)

    def get_number_of_clicks_by_session_id(self, session_id):
        query = {"session_id": session_id}
        return self.db["clicks"].count_documents(query)

    def get_average_price_for_bid_by_user_id(self, user_id):
        query = {"user_id": user_id, "win": "TRUE"}
        projection = {"_id": 0, "bid": 1}
        bids = list(self.db["requests"].find(query, projection))
        bids_sum = 0
        for bid in bids:
            bids_sum += float(bid['bid'])
        if len(bids) == 0:
            return 0
        average_price = bids_sum / len(bids)
        return average_price

    def is_user_id_already_exists_in_requests(self, user_id):
        if self.db["requests"].count_documents({"user_id": user_id}) > 0:
            return True
        else:
            return False

    def get_median_duration_by_user_id(self, user_id):
        session_ids = list(self.get_session_ids_by_user_id(user_id))
        duration_list = []
        for session_id in session_ids:
            durations = self.get_duration_by_session_id(session_id["session_id"])
            for duration_dict in durations:
                duration_list.append(float(duration_dict["duration"]))
        if len(duration_list) == 0:
            return 0
        else:
            return median(duration_list)

    def get_max_time_till_click_by_user_id(self, user_id):
        session_ids = list(self.get_session_ids_by_user_id(user_id))
        time_passed_list = []
        for session_id in session_ids:
            time_passed_cursor = self.get_time_passed_by_session_id(session_id["session_id"])
            for time_passed_dict in time_passed_cursor:
                time_passed_list.append(float(time_passed_dict["time"]))
        if len(time_passed_list) == 0:
            return 0
        else:
            return max(time_passed_list)

    def get_time_passed_by_session_id(self, session_id):
        query = {"session_id": session_id}
        projection = {"_id": 0, "time": 1}
        return list(self.db["clicks"].find(query, projection))

    def get_duration_by_session_id(self, session_id):
        query = {"session_id": session_id}
        projection = {"_id": 0, "duration": 1}
        return list(self.db["impressions"].find(query, projection))

    # --------------------- Methods for /sessionId route helper ---------------------------------
    def get_timestamp_from_requests_by_session_id(self, session_id):
        query = {"session_id": session_id}
        projection = {"_id": 0, "timeStamp": 1}
        timestamp_list = list(self.db["requests"].find(query, projection))
        if len(timestamp_list) == 0:
            return 0
        return timestamp_list[0]["timeStamp"]

    def get_latest_timestamp_in_any_table_by_session_id(self, session_id):
        timestamp_list = [self.get_timestamp_from_requests_by_session_id(session_id),
                          self.get_timestamp_from_impressions_by_session_id(session_id),
                          self.get_timestamp_from_clicks_by_session_id(session_id)]
        mapped_list = map(lambda x: int(x), timestamp_list)
        return str(max(mapped_list))

    def get_partner_name_by_session_id(self, session_id):
        query = {"session_id": session_id}
        projection = {"_id": 0, "partner": 1}
        partner_list = list(self.db["requests"].find(query, projection))
        if len(partner_list) == 0:
            return None
        return partner_list[0]["partner"]

    # -------------------------------- helper functions ------------------------------------------

    def get_timestamp_from_impressions_by_session_id(self, session_id):
        query = {"session_id": session_id}
        projection = {"_id": 0, "timeStamp": 1}
        timestamp_list = list(self.db["impressions"].find(query, projection))
        if len(timestamp_list) == 0:
            return 0
        return timestamp_list[0]["timeStamp"]

    def get_timestamp_from_clicks_by_session_id(self, session_id):
        query = {"session_id": session_id}
        projection = {"_id": 0, "timeStamp": 1}
        timestamp_list = list(self.db["clicks"].find(query, projection))
        if len(timestamp_list) == 0:
            return 0
        return timestamp_list[0]["timeStamp"]

    def is_session_id_already_exists_in_requests(self, session_id):
        if self.db["requests"].count_documents({"session_id": session_id}) > 0:
            return True
        else:
            return False


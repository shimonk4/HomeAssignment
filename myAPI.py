from dataBase import DataBase
import csv

# path for the folder which contains the following csv:
# requests.csv, impressions.csv and clicks.csv
CSV_FILES_BASE = "./csv_files/"


class MyAPI:
    # --------------------------------------- class methods ---------------------------------------
    def __init__(self):
        # connect to MongoDB API
        self.database = DataBase()
        self.is_server_on = False

    def start_loading(self):
        # load CSVs to the data base from the CSV_FILES_BASE..
        print("Loading csv files to the database...")
        print("Loading requests file...")
        self.load_requests()
        print("Loading impressions file...")
        self.load_impressions()
        print("Loading clicks file...")
        self.load_clicks()
        print("Done loading! Ready to get requests")
        self.is_server_on = True

    # --------------------- Methods for /userStats route helper ---------------------------------
    def get_num_of_requests_by_user_id(self, user_id):
        return self.database.get_num_requests_by_user_id(user_id)

    def get_num_of_impressions_by_user_id(self, user_id):
        session_ids = self.database.get_session_ids_by_user_id(user_id)
        total_number_of_impression = 0
        for session_id in session_ids:
            total_number_of_impression += self.database.get_number_of_impressions_by_session_id(session_id["session_id"])
        return total_number_of_impression

    def get_num_of_clicks_by_user_id(self, user_id):
        session_ids = self.database.get_session_ids_by_user_id(user_id)
        total_number_of_clicks = 0
        for session_id in session_ids:
            total_number_of_clicks += self.database.get_number_of_clicks_by_session_id(session_id["session_id"])
        return total_number_of_clicks

    def get_average_price_for_bid_by_user_id(self, user_id):
        return self.database.get_average_price_for_bid_by_user_id(user_id)

    def get_median_impression_duration_by_user_id(self, user_id):
        return self.database.get_median_duration_by_user_id(user_id)

    def get_max_time_passed_till_click(self, user_id):
        return self.database.get_max_time_till_click_by_user_id(user_id)

    # --------------------- Methods for /sessionId route helper ---------------------------------
    def get_timestamp_from_requests_by_session_id(self, session_id):
        return self.database.get_timestamp_from_requests_by_session_id(session_id)

    def get_latest_timestamp_in_any_table_by_session_id(self, session_id):
        return self.database.get_latest_timestamp_in_any_table_by_session_id(session_id)

    def get_partner_name_by_session_id(self, session_id):
        return self.database.get_partner_name_by_session_id(session_id)

    # --------------------------------------- aux methods ---------------------------------------

    def get_session_ids_by_user_id(self, user_id):
        return self.database.get_session_ids_by_user_id(user_id)

    # load requests from the requests.csv file into mongo DB
    def load_requests(self):
        with open(CSV_FILES_BASE + "requests.csv", "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            for line in csv_reader:
                self.database.add_request({"timeStamp": line[0], "session_id": line[1], "partner": line[2],
                                           "user_id": line[3], "bid": line[4], "win": line[5]})

    # load clicks from the requests.csv file into mongo DB
    def load_clicks(self):
        with open(CSV_FILES_BASE + "clicks.csv", "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            for line in csv_reader:
                self.database.add_click({"timeStamp": line[0], "session_id": line[1], "time": line[2]})

    # load impressions from the requests.csv file into mongo DB
    def load_impressions(self):
        with open(CSV_FILES_BASE + "impressions.csv", "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            for line in csv_reader:
                self.database.add_impression({"timeStamp": line[0], "session_id": line[1], "duration": line[2]})


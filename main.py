from myAPI import MyAPI
from flask import Flask, jsonify
import threading

app = Flask(__name__)
myApi = MyAPI()


def start_loading():
    myApi.start_loading()


@app.before_first_request
def load_all_files():
    if not myApi.is_server_on:
        x = threading.Thread(target=start_loading)
        x.start()
    else:
        return "<h1>The CSV files already loaded..</h1>"


@app.route('/')
def home_page():
    return "<h1>Welcome To My Home Assignment</h1>"


@app.route('/keepalive', methods=['GET', 'POST'])
def keep_alive():
    if myApi.is_server_on:
        res = "<h1>The Service up and ready for query !!</h1>"
    else:
        res = "<h1>The Service is not ready for query right now,please try again later...</h1>"
    return res


@app.route('/userStats/<user_id>', methods=['GET', 'POST'])
def user_stats(user_id):
    if myApi.is_server_on:
        if myApi.database.is_user_id_already_exists_in_requests(user_id):
            return jsonify({"Num of requests": myApi.get_num_of_requests_by_user_id(user_id),
                            "Num of impressions": myApi.get_num_of_impressions_by_user_id(user_id),
                            "Num of clicks": myApi.get_num_of_clicks_by_user_id(user_id),
                            "Average price for bid": round(myApi.get_average_price_for_bid_by_user_id(user_id), 2),
                            "Median impression duration": myApi.get_median_impression_duration_by_user_id(user_id),
                            "Max time passed till click": myApi.get_max_time_passed_till_click(user_id)})
        else:
            return "<h1>Invalid user_id, please enter a valid one.</h1>"
    else:
        return "<h1>The Service is not ready for query right now,please try again later...</h1>"


@app.route('/sessionId/<sess_id>', methods=['GET', 'POST'])
def session_id(sess_id):
    if myApi.is_server_on:
        if myApi.database.is_user_id_already_exists_in_requests(sess_id):
            return jsonify({"Begin": myApi.get_timestamp_from_requests_by_session_id(sess_id),
                            "Finish": myApi.get_latest_timestamp_in_any_table_by_session_id(sess_id),
                            "Partner Name": myApi.get_partner_name_by_session_id(sess_id)})
        else:
            return "<h1>Invalid session_id, please enter a valid one.</h1>"
    else:
        return "<h1>The Service is not ready for query right now,please try again later...</h1>"


if __name__ == "__main__":
    app.run(debug=True)






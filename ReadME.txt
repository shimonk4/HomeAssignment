Welcome to my home assignment:

Requirements:
1. First create your own python environment and then download program code.
2. Run the command 'pip install -r requirements.txt' at your new env ( or install the packages manually one by one).
3. Create folder in the same directory when your code is called csv_files, and put the csv files in there.
4. You have to download RoboMongo on your machine in order to set the database as needed (and connect in order to see the database updated).

* Note: If you want to see the database updated while running the server you have to refresh it on the robomongo software.

How To Use:

- Run the flask server (from the IDE or even from the terminal).
- The Server is running and all you have to do is to click on the link to your localhost server ( by default).
- Then, you can send a rest api requests as explained in the assignment pdf.

( 127.0.0.1:5000 - This is the home page.
   127.0.0.1:5000/keepalive - does not get any input and return whether the api ready to get requests.
   127.0.0.1:5000/userStats/* - input is a valid user_id in order to get his stats.
   127.0.0.1:5000/sessionId/* - input is a valid session_id in order to get details about this session. )

Suggested improvements:
- I think we should add another routes for adding information to every csv file or even insert directly to the database. ( when getting more information)
- Maybe add function which show the last K sessions for a given user_id in the last M minutes.
- Add some css designed to look good. ( buttons, div etc..)

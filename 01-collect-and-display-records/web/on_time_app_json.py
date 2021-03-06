from pymongo import MongoClient
from bson import json_util
from flask import Flask, render_template, request

# Set up Flask and Mongo
app = Flask(__name__)
client = MongoClient()

# Controller: Fetch one flight record and display
@app.route("/on_time_performance")
def on_time_performance():
    # Parse incoming request:
    carrier = request.args.get('Carrier')
    flight_date = request.args.get('FlightDate')
    flight_num = request.args.get('FlightNum')

    # Search for flight:
    flight = client.agile_data_science.on_time_performance.find_one(
        {
            'Carrier': carrier,
            'FlightDate': flight_date,
            'FlightNum': flight_num
        }
    )

    # Dump as JSON:
    return json_util.dumps(flight)

if __name__ == "__main__":
    app.run(debug=True)

from pymongo import MongoClient
from bson import json_util
from flask import Flask, render_template, request

# Set up Flask and Mongo
app = Flask(__name__)
client = MongoClient()

RECORDS_PER_PAGE = 20

def get_navigation_offsets(start, end, increment):
    offsets = {}
    offsets['Previous'] = {
        'start': max(0, start - increment),
        'end': max(0, end - increment)
    }
    offsets['Next'] = {
        'start': start + increment,
        'end':   end + increment
    }

    return offsets

# Controller: Fetch all flights between cities on a given day and display them
@app.route("/flights/<origin>/<dest>/<flight_date>")
def list_flights(origin, dest, flight_date):
    # Parse pagination params:
    start = request.args.get("start") or 0
    start = int(start)
    end = request.args.get("end") or RECORDS_PER_PAGE
    end = int(end)
    num_records = end - start

    # Get matching flights:
    flights = client.agile_data_science.on_time_performance.find(
        {
            'Origin': origin,
            'Dest': dest,
            'FlightDate': flight_date
        },
        sort = [
            ('DepTime', 1),
            ('ArrTime', 1)
        ]
    )
    # Total number of results:
    flight_count = flights.count()

    flights.skip(start).limit(num_records)

    # Render template:
    return render_template(
        'flights.html',
        flights=flights,
        flight_date=flight_date,
        flight_count=flight_count,
        nav_path = request.path,
        nav_offsets = get_navigation_offsets(start, end, RECORDS_PER_PAGE)
    )

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
    return render_template('flight.html', flight=flight)

if __name__ == "__main__":
    app.run(debug=True)

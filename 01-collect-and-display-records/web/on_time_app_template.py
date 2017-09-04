import re

from pymongo import MongoClient
from bson import json_util

from pyelasticsearch import ElasticSearch

from flask import Flask, render_template, request

# Configuration:
ELASTIC_URL = "http://localhost:9200/agile_data_science"
RECORDS_PER_PAGE = 20

# Utilities:
def parse_es_search_results(results):
    """
    """
    records = []

    if results["hits"] and results["hits"]["hits"]:
        total = results["hits"]["total"]

        for hit in results["hits"]["hits"]:
            records.append(hit["_source"])

    return (records, total)

def get_navigation_offsets(start, end, increment):
    offsets = []
    offsets.append(
        ('Previous', {
            'start': max(0, start - increment),
            'end': max(0, end - increment)
        })
    )
    offsets.append(
        ('Next', {
            'start': start + increment,
            'end':   end + increment
        })
    )

    return offsets

def strip_place(url):
    try:
        path = re.match('(.+)&start=.+&end=.+', url).group(1)
    except AttributeError, e:
        return url
    return path

# Set up Flask and Mongo
app = Flask(__name__)
elastic = ElasticSearch(ELASTIC_URL)
mongodb = MongoClient()

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
    flights = mongodb.agile_data_science.on_time_performance.find(
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
    flight = mongodb.agile_data_science.on_time_performance.find_one(
        {
            'Carrier': carrier,
            'FlightDate': flight_date,
            'FlightNum': flight_num
        }
    )

    # Dump as JSON:
    return render_template('flight.html', flight=flight)

@app.route("/flights/search/")
def search_flights():
    # Search parameters:
    carrier = request.args.get('Carrier')
    flight_date = request.args.get('FlightDate')
    origin = request.args.get('Origin')
    dest = request.args.get('Dest')
    tail_number = request.args.get('TailNum')
    flight_number = request.args.get('FlightNum')

    # Pagination parameters
    start = request.args.get('start') or 0
    start = int(start)
    end = request.args.get('end') or RECORDS_PER_PAGE
    end = int(end)

    # Navigation path and offset setup
    nav_path = strip_place(request.url)
    nav_offsets = get_navigation_offsets(start, end, RECORDS_PER_PAGE)

    # Build the base of our elasticsearch query
    query = {
        'query': {
            'bool': {
                'must': []
            }
        },
        'sort': [
            {'FlightDate': {'order': 'asc', 'ignore_unmapped' : True} },
            {'DepTime': {'order': 'asc', 'ignore_unmapped' : True} },
            {'Carrier': {'order': 'asc', 'ignore_unmapped' : True} },
            {'FlightNum': {'order': 'asc', 'ignore_unmapped' : True} },
            '_score'
        ],
        'from': start,
        'size': RECORDS_PER_PAGE
    }

    # Add any search parameters present
    if carrier:
        query['query']['bool']['must'].append({'match': {'Carrier': carrier}})
    if flight_date:
        query['query']['bool']['must'].append({'match': {'FlightDate': flight_date}})
    if origin:
        query['query']['bool']['must'].append({'match': {'Origin': origin}})
    if dest:
        query['query']['bool']['must'].append({'match': {'Dest': dest}})
    if tail_number:
        query['query']['bool']['must'].append({'match': {'TailNum': tail_number}})
    if flight_number:
        query['query']['bool']['must'].append({'match': {'FlightNum': flight_number}})

    # Query elasticsearch, process to get records and count
    print("QUERY")
    print(carrier, flight_date, origin, dest, tail_number, flight_number)
    print(json.dumps(query))
    results = elastic.search(query)
    flights, flight_count = process_search(results)

    # Persist search parameters in the form template
    return render_template(
        'search.html',
        flights=flights,
        flight_date=flight_date,
        flight_count=flight_count,
        nav_path=nav_path,
        nav_offsets=nav_offsets,
        carrier=carrier,
        origin=origin,
        dest=dest,
        tail_number=tail_number,
        flight_number=flight_number
    )

if __name__ == "__main__":
    app.run(debug=True)

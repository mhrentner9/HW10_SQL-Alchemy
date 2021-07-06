from flask import Flask, jsonify
import datetime as dt
import numpy as np
import pandas as pd
# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Instructions/Resources/hawaii.sqlite")

# Reflect Database into ORM classes
Base = automap_base()
Base.prepare(engine, reflect=True)
Base.classes.keys()

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

#Create a Session
session = Session(engine)

# Flask Setup
app = Flask(__name__)

# Flask Routes
# Home Page and List All Routes Available
@app.route("/")
def home():
    return(
        f"(Hello! SQLAlchemy Challenge Climate App). <br><br>"
        f"Available Routes: <br>"

        f"/api/v1.0/precipitation<br/>"
        f"Returns dates and temperature from the last year in data set. <br><br>"

        f"/api/v1.0/stations<br/>"
        f"Returns a list of stations. <br><br>"

        f"/api/v1.0/tobs<br/>"
        f"Returns list of Temperature Observations for last year in data set. <br><br>"

        f"/api/v1.0/yyyy-mm-dd/<br/>"
        f"Returns an Average, Max, and Min temperatures for a given start date.<br><br>"

        f"/api/v1.0/yyyy-mm-dd/yyyy-mm-dd/<br/>"
        f"Returns an Average, Max, and Min temperatures for a given date range."
   ) 

# /api/v1.0/precipitation
# Convert query results to a dictionary using date as the key and prcp as the value
# Return the JSON representation of your dictionary

@app.route("/api/v1.0/precipitation")
def precipitation():
    session = Session(engine)
    last_date = session.query(func.max(Measurement.date)).first()
    last_date2=str(last_date)
    year = int(last_date2[2]+last_date2[3]+last_date2[4]+last_date2[5])
    month = int(last_date2[7]+last_date2[8])
    day = int(last_date2[10]+last_date2[11])
    query_date = dt.date(year, month, day) - dt.timedelta(days=365)
    maxdate = dt.date(year, month, day)
    prcp_list = session.query(Measurement.date, Measurement.prcp).\
    filter(Measurement.date > query_date).filter(Measurement.date <= maxdate).\
    all()
    session.close()
    return jsonify(prcp_list)

# /api/v1.0/stations
# Return the JSON list of stations in the dataset

@app.route("/api/v1.0/stations")
def stations():
    session = Session(engine)
    stationhits = session.query(Measurement.station, Station.name, func.count(Measurement.station)).\
    filter(Measurement.station == Station.station).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()
    session.close()
    return jsonify(stationhits)

# /api/v1.0/tobs
# Query the dates and temperature observations of the most active station for the last year of data.
# Return a JSON list of temperature observations (TOBS) for the previous year.
      
@app.route("/api/v1.0/tobs")
def tobs():
    session = Session(engine)
    results = session.query(Station.name, Measurement.date, Measurement.tobs).\
        filter(Measurement.date >= "2016-08-24", Measurement.date <= "2017-08-23").\
        all()
    tobs_list = []
    for result in results:
        row = {}
        row["Station"] = result[0]
        row["Date"] = result[1]
        row["Temp"] = int(result[2])
        tobs_list.append(row)
    session.close()
    return jsonify(tobs_list)

    
@app.route('/api/v1.0/<date>/')
def given_date(date):
    session = Session(engine)
    results = session.query(func.avg(Measurement.tobs), func.max(Measurement.tobs), func.min(Measurement.tobs)).\
        filter(Measurement.date >= date).all()

    date_list = []
    for result in results:
        row = {}
        row['Start Date'] = date
        row['End Date'] = '2017-08-23'
        row['Average Temp'] = float(result[0])
        row['Highest Temp'] = float(result[1])
        row['Lowest Temp'] = float(result[2])
        date_list.append(row)
    session.close()
    return jsonify(date_list)

# /api/v1.0/<start> and /api/v1.0/<start>/<end>
# Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range.
# When given the start only, calculate TMIN, TAVG, and TMAX for all dates greater than and equal to the start date.
# When given the start and the end date, calculate the TMIN, TAVG, and TMAX for dates between the start and end date inclusive.

@app.route('/api/v1.0/<start_date>/<end_date>/')
def query_dates(start_date, end_date):
    session = Session(engine)
    results = session.query(func.avg(Measurement.tobs), func.max(Measurement.tobs), func.min(Measurement.tobs)).\
        filter(Measurement.date >= start_date, Measurement.date <= end_date).all()

    range_list = []
    for result in results:
        row = {}
        row['Start Date'] = start_date
        row['End Date'] = end_date
        row['Average Temp'] = float(result[0])
        row['Highest Temp'] = float(result[1])
        row['Lowest Temp'] = float(result[2])
        range_list.append(row)
    session.close()
    return jsonify(range_list)
    
if __name__ == "__main__":
    app.run(debug=True)

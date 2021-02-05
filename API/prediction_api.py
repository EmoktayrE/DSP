from datetime import datetime as dt
from datetime import timedelta
from flask import Flask, jsonify, request, Response
from sqlalchemy.exc import IntegrityError

import json
import pandas as pd

from settings import *
from cmsamodels import *
from gvbmodels import *
import cmsapredictor
import gvbpredictor


#GET /cmsa/latest
@app.route('/cmsa/latest')
def get_latest_cmsa_readings():
	return jsonify({'readings': CMSAReading.get_latest_readings()})

#GET /gvb/latest
@app.route('/gvb/latest')
def get_latest_gvb_readings():
	return jsonify({'readings': GVBReading.get_latest_readings()})

#GET /cmsa/latest/sensor
@app.route('/cmsa/latest/<sensor>')
def get_latest_cmsa_reading_by_sensor(sensor):
	return jsonify(CMSAReading.get_latest_reading_for_sensor(sensor))

#GET /gvb/latest/station
@app.route('/gvb/latest/<station>')
def get_latest_gvb_reading_by_station(station):
	return jsonify(GVBReading.get_latest_reading_for_station(station))

#GET /cmsa/predictions/1h
@app.route('/cmsa/predictions/1h')
def get_cmsa_predictions_1h():
	return jsonify({'predictions': CMSAPrediction.get_latest_predictions(1)})

#GET /cmsa/predictions/2h
@app.route('/cmsa/predictions/2h')
def get_cmsa_predictions_2h():
	return jsonify({'predictions': CMSAPrediction.get_latest_predictions(2)})

#GET /gvb/predictions/1h
@app.route('/gvb/predictions/1h')
def get_gvb_predictions_1h():
	return jsonify({'predictions': GVBPrediction.get_latest_predictions(1)})

#GET /gvb/predictions/2h
@app.route('/gvb/predictions/2h')
def get_gvb_predictions_2h():
	return jsonify({'predictions': GVBPrediction.get_latest_predictions(2)})

#POST /cmsa
@app.route('/cmsa', methods=['POST'])
def add_cmsa_reading():
	request_data = request.get_json()
	if valid_cmsa_reading_object(request_data):
		try:
			CMSAReading.add_reading(request_data['sensor'], request_data['timestamp_rounded'], request_data['total_count'], request_data['count_down'], request_data['count_up'], request_data['density_avg'], request_data['speed_avg'])
			response = Response(response="", status=201, mimetype="application/json")
			response.headers['Location'] = "/cmsa/latest/" + str(request_data['sensor'])
		except IntegrityError:
			error_msg = json.dumps({"error":f"Record for {request_data['sensor']} at {request_data['timestamp_rounded']} already exists"})
			response = Response(response=error_msg, status=409, mimetype="application/json")
		except ValueError as ve:
			error_msg = json.dumps({"error":str(ve)})
			response = Response(response=error_msg, status=422, mimetype="application/json")
		return response
	else:
		return Response(response=json.dumps({"error":"Invalid CMSA reading object"}), status=400, mimetype="application/json")

#POST /gvb
@app.route('/gvb', methods=['POST'])
def add_gvb_reading():
	request_data = request.get_json()
	if valid_gvb_reading_object(request_data):
		try:
			GVBReading.add_reading(request_data['station'], request_data['timestamp'], request_data['total_count'])
			response = Response(response="", status=201, mimetype="application/json")
			response.headers['Location'] = "/gvb/latest/" + str(request_data['station'])
		except IntegrityError:
			error_msg = json.dumps({"error":f"Record for {request_data['station']} at {request_data['timestamp']} already exists"})
			response = Response(response=error_msg, status=409, mimetype="application/json")
		except ValueError as ve:
			error_msg = json.dumps({"error":str(ve)})
			response = Response(response=error_msg, status=422, mimetype="application/json")
		return response
	else:
		return Response(response=json.dumps({"error":"Invalid GVB reading object"}), status=400, mimetype="application/json")

#POST /cmsa/predict
@app.route('/cmsa/predict', methods=['POST'])
def add_cmsa_reading_and_predict():
	request_data = request.get_json()
	if valid_cmsa_reading_object(request_data):
		try:
			CMSAReading.add_reading(request_data['sensor'], request_data['timestamp_rounded'], request_data['total_count'], request_data['count_down'], request_data['count_up'], request_data['density_avg'], request_data['speed_avg'])
			predicted_total_count_1h, predicted_speed_avg_1h = predict_cmsa_for_sensor(sensor=request_data['sensor'], timestamp=request_data['timestamp_rounded'], offset_hours=1)
			predicted_total_count_2h, predicted_speed_avg_2h = predict_cmsa_for_sensor(sensor=request_data['sensor'], timestamp=request_data['timestamp_rounded'], offset_hours=2)
			CMSAPrediction.add_prediction(request_data['sensor'], request_data['timestamp_rounded'], 1, int(predicted_total_count_1h.max()), predicted_speed_avg_1h.max())
			CMSAPrediction.add_prediction(request_data['sensor'], request_data['timestamp_rounded'], 2, int(predicted_total_count_2h.max()), predicted_speed_avg_2h.max())
			response = Response(response="", status=201, mimetype="application/json")
			response.headers['Location'] = "/cmsa/latest/" + str(request_data['sensor'])
		except IntegrityError:
			error_msg = json.dumps({"error":f"Record for {request_data['sensor']} at {request_data['timestamp_rounded']} already exists"})
			response = Response(response=error_msg, status=409, mimetype="application/json")
		except ValueError as ve:
			error_msg = json.dumps({"error":str(ve)})
			response = Response(response=error_msg, status=422, mimetype="application/json")
		return response
	else:
		return Response(response=json.dumps({"error":"Invalid CMSA reading object"}), status=400, mimetype="application/json")

#POST /gvb/predict
@app.route('/gvb/predict', methods=['POST'])
def add_gvb_reading_and_predict():
	request_data = request.get_json()
	if valid_gvb_reading_object(request_data):
		try:
			GVBReading.add_reading(request_data['station'], request_data['timestamp'], request_data['total_count'])
			predicted_total_count_1h = predict_gvb_for_station(station=request_data['station'], timestamp=request_data['timestamp'], offset_hours=1)
			predicted_total_count_2h = predict_gvb_for_station(station=request_data['station'], timestamp=request_data['timestamp'], offset_hours=2)
			GVBPrediction.add_prediction(request_data['station'], request_data['timestamp'], 1, int(predicted_total_count_1h.max()))
			GVBPrediction.add_prediction(request_data['station'], request_data['timestamp'], 2, int(predicted_total_count_2h.max()))
			response = Response(response="", status=201, mimetype="application/json")
			response.headers['Location'] = "/gvb/latest/" + str(request_data['station'])
		except IntegrityError:
			error_msg = json.dumps({"error":f"Record for {request_data['station']} at {request_data['timestamp']} already exists"})
			response = Response(response=error_msg, status=409, mimetype="application/json")
		except ValueError as ve:
			error_msg = json.dumps({"error":str(ve)})
			response = Response(response=error_msg, status=422, mimetype="application/json")
		return response
	else:
		return Response(response=json.dumps({"error":"Invalid GVB reading object"}), status=400, mimetype="application/json")

def predict_cmsa_for_sensor(sensor, timestamp, offset_hours):
	# determine starting timestamp
	from_timestamp = dt.strptime(timestamp,'%Y-%m-%dT%H:%M:%S%z') - timedelta(days=7)
	cmsa_sensor_df = pd.DataFrame.from_records(CMSAReading.get_readings_for_sensor_from_timestamp(sensor, from_timestamp), columns=['sensor','timestamp','total_count','count_down','count_up','density_avg','speed_avg'])
	# turn timestamps into datetime, set it as index
	cmsa_sensor_df['timestamp'] = pd.to_datetime(cmsa_sensor_df["timestamp"], infer_datetime_format=True)
	cmsa_sensor_df = cmsa_sensor_df.set_index('timestamp')
	# define frequency
	cmsa_sensor_df = cmsa_sensor_df.asfreq('15T')
	cmsa_sensor_df.index = pd.DatetimeIndex(cmsa_sensor_df.index.values, freq='15T')
	# fill any missing values
	cmsa_sensor_df['total_count'].interpolate(method='linear', inplace=True)
	cmsa_sensor_df['speed_avg'].interpolate(method='linear', inplace=True)
	return (cmsapredictor.predict_column_at(df=cmsa_sensor_df, column='total_count', offset=4*offset_hours), cmsapredictor.predict_column_at(df=cmsa_sensor_df, column='speed_avg', offset=4*offset_hours))

def predict_gvb_for_station(station, timestamp, offset_hours):
	# determine starting timestamp
	from_timestamp = dt.strptime(timestamp,'%Y-%m-%dT%H:%M:%S%z') - timedelta(days=14)
	gvb_station_df = pd.DataFrame.from_records(GVBReading.get_readings_for_station_from_timestamp(station, from_timestamp), columns=['station','timestamp','total_count'])
	# turn timestamps into datetime, set it as index
	gvb_station_df['timestamp'] = pd.to_datetime(gvb_station_df["timestamp"], infer_datetime_format=True)
	gvb_station_df = gvb_station_df.set_index('timestamp')
	# define frequency
	gvb_station_df = gvb_station_df.asfreq('1H')
	gvb_station_df.index = pd.DatetimeIndex(gvb_station_df.index.values, freq='1H')
	# fill any missing values
	gvb_station_df['total_count'].interpolate(method='linear', inplace=True)
	return gvbpredictor.predict(df=gvb_station_df, offset=offset_hours)

def valid_cmsa_reading_object(reading_object):
	return ("sensor" in reading_object 
			and "timestamp_rounded" in reading_object 
			and "total_count" in reading_object
			and "count_down" in reading_object
			and "count_up" in reading_object
			and "density_avg" in reading_object
			and "speed_avg" in reading_object)

def valid_gvb_reading_object(reading_object):
	return ("station" in reading_object 
			and "timestamp" in reading_object 
			and "total_count" in reading_object)


app.run(port=5000)

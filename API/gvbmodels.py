from datetime import datetime as dt
from datetime import timedelta
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func, and_

import json

from settings import app


db = SQLAlchemy(app)

VALID_STATIONS = ['Centraal Station','Dam','Nieuwezijds Kolk','Nieuwmarkt','Rokin','Spui']


class GVBReading(db.Model):
	__tablename__ = 'gvb_readings'
	id = db.Column(db.Integer, primary_key=True)
	station = db.Column(db.String(20), nullable=False)
	timestamp = db.Column(db.DateTime, nullable=False)
	total_count = db.Column(db.Integer, nullable=False)
	db.UniqueConstraint(station, timestamp)
	
	
	def json(self):
		return {'station': self.station,
				'timestamp': str(self.timestamp),
				'total_count': self.total_count
				}

	def add_reading(_station, _timestamp, _total_count):
		if _station in VALID_STATIONS:
			dt_timestamp = dt.strptime(_timestamp,'%Y-%m-%dT%H:%M:%S%z')
			new_reading = GVBReading(station=_station, timestamp=dt_timestamp, total_count=_total_count)
			db.session.add(new_reading)
			db.session.commit()
		else:
			raise ValueError(f'{_station} is not currently supported')

	def get_all_readings():
		return [GVBReading.json(reading) for reading in GVBReading.query.all()]

	def get_latest_readings():
		subq = db.session.query(GVBReading.station, func.max(GVBReading.timestamp).label('maxtsp')).group_by(GVBReading.station).subquery('t2')
		return [GVBReading.json(reading) for reading in db.session.query(GVBReading).join(subq, and_(GVBReading.station==subq.c.station, GVBReading.timestamp==subq.c.maxtsp))]

	def get_latest_reading_for_station(_station):
		return GVBReading.json(GVBReading.query.filter_by(station=_station).order_by(GVBReading.timestamp.desc()).first())

	def get_readings_for_station_from_timestamp(_station, from_timestamp):
		return [GVBReading.json(reading) for reading in GVBReading.query.filter((GVBReading.station==_station)&(GVBReading.timestamp>=from_timestamp)).all()]

	def __repr__(self):
		return json.dumps(self.json())


class GVBPrediction(db.Model):
	__tablename__ = 'gvb_predictions'
	id = db.Column(db.Integer, primary_key=True)
	station = db.Column(db.String(20), nullable=False)
	timestamp = db.Column(db.DateTime, nullable=False)
	timestamp_offset = db.Column(db.Integer, nullable=False)
	total_count = db.Column(db.Integer, nullable=False)
	db.UniqueConstraint(station, timestamp, timestamp_offset)
	

	def json(self):
		return {'station': self.station,
				'timestamp': str(self.timestamp),
				'timestamp_offset': self.timestamp_offset,
				'total_count': self.total_count
				}

	def add_prediction(_station, _timestamp, _timestamp_offset, _total_count):
		if _station in VALID_STATIONS:
			dt_timestamp = dt.strptime(_timestamp,'%Y-%m-%dT%H:%M:%S%z') + timedelta(hours=_timestamp_offset)
			new_prediction = GVBPrediction(station=_station, timestamp=dt_timestamp, timestamp_offset=_timestamp_offset, total_count=max(0,_total_count))
			db.session.add(new_prediction)
			db.session.commit()
		else:
			raise ValueError(f'{_station} is not currently supported')

	def get_latest_predictions(_timestamp_offset):
		subq = db.session.query(GVBPrediction.station, GVBPrediction.timestamp_offset, func.max(GVBPrediction.timestamp).label('maxtsp')).group_by(GVBPrediction.station, GVBPrediction.timestamp_offset).filter(GVBPrediction.timestamp_offset==_timestamp_offset).subquery('t2')
		return [GVBPrediction.json(reading) for reading in db.session.query(GVBPrediction).filter(GVBPrediction.timestamp_offset==_timestamp_offset).join(subq, and_(GVBPrediction.station==subq.c.station, GVBPrediction.timestamp==subq.c.maxtsp))]

	def __repr__(self):
		return json.dumps(self.json())

from datetime import datetime as dt
from datetime import timedelta
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func, and_

import json

from settings import app


db = SQLAlchemy(app)

VALID_SENSORS = ['GAAM-01-AlbertCuypstraat','GABM-01','GABM-02','GABM-03','GABM-04','GABM-05','GABM-06','GACM-01','GACM-02','GACM-03','GACM-04','GAVM-02-Stadhouderskade','GAVM-05-Amstelveenseweg','GAWW-01','GAWW-02','GAWW-03','GAWW-04','GAWW-05','GAWW-07','GKS-01-Kalverstraat','GVCV-01','GVCV-03','GVCV-04','GVCV-05','GVCV-06','GVCV-07','GVCV-08']


class CMSAReading(db.Model):
	__tablename__ = 'cmsa_readings'
	id = db.Column(db.Integer, primary_key=True)
	sensor = db.Column(db.String(20), nullable=False)
	timestamp = db.Column(db.DateTime, nullable=False)
	total_count = db.Column(db.Integer, nullable=False)
	count_down = db.Column(db.Integer, nullable=False)
	count_up  = db.Column(db.Integer, nullable=False)
	density_avg = db.Column(db.Float)
	speed_avg = db.Column(db.Float)
	db.UniqueConstraint(sensor, timestamp)
	
	
	def json(self):
		return {'sensor': self.sensor,
				'timestamp': str(self.timestamp),
				'total_count': self.total_count,
				'count_down': self.count_down,
				'count_up': self.count_up,
				'density_avg': self.density_avg,
				'speed_avg': self.speed_avg
				}

	def add_reading(_sensor, _timestamp, _total_count, _count_down, _count_up, _density_avg, _speed_avg):
		if _sensor in VALID_SENSORS:
			dt_timestamp = dt.strptime(_timestamp,'%Y-%m-%dT%H:%M:%S%z')
			new_reading = CMSAReading(sensor=_sensor, timestamp=dt_timestamp, total_count=_total_count, count_down=_count_down, count_up=_count_up, density_avg=_density_avg, speed_avg=_speed_avg)
			db.session.add(new_reading)
			db.session.commit()
		else:
			raise ValueError(f'{_sensor} is not currently supported')

	def get_all_readings():
		return [CMSAReading.json(reading) for reading in CMSAReading.query.all()]

	def get_latest_readings():
		subq = db.session.query(CMSAReading.sensor, func.max(CMSAReading.timestamp).label('maxtsp')).group_by(CMSAReading.sensor).subquery('t2')
		return [CMSAReading.json(reading) for reading in db.session.query(CMSAReading).join(subq, and_(CMSAReading.sensor==subq.c.sensor, CMSAReading.timestamp==subq.c.maxtsp))]

	def get_latest_reading_for_sensor(_sensor):
		return CMSAReading.json(CMSAReading.query.filter_by(sensor=_sensor).order_by(CMSAReading.timestamp.desc()).first())

	def get_readings_for_sensor_from_timestamp(_sensor, from_timestamp):
		return [CMSAReading.json(reading) for reading in CMSAReading.query.filter((CMSAReading.sensor==_sensor)&(CMSAReading.timestamp>=from_timestamp)).all()]

	def __repr__(self):
		return json.dumps(self.json())


class CMSAPrediction(db.Model):
	__tablename__ = 'cmsa_predictions'
	id = db.Column(db.Integer, primary_key=True)
	sensor = db.Column(db.String(20), nullable=False)
	timestamp = db.Column(db.DateTime, nullable=False)
	timestamp_offset = db.Column(db.Integer, nullable=False)
	total_count = db.Column(db.Integer, nullable=False)
	speed_avg = db.Column(db.Float)
	db.UniqueConstraint(sensor, timestamp, timestamp_offset)
	

	def json(self):
		return {'sensor': self.sensor,
				'timestamp': str(self.timestamp),
				'timestamp_offset': self.timestamp_offset,
				'total_count': self.total_count,
				'speed_avg': self.speed_avg
				}

	def add_prediction(_sensor, _timestamp, _timestamp_offset, _total_count, _speed_avg):
		if _sensor in VALID_SENSORS:
			dt_timestamp = dt.strptime(_timestamp,'%Y-%m-%dT%H:%M:%S%z') + timedelta(hours=_timestamp_offset)
			new_prediction = CMSAPrediction(sensor=_sensor, timestamp=dt_timestamp, timestamp_offset=_timestamp_offset, total_count=max(0,_total_count), speed_avg=max(0.0,_speed_avg))
			db.session.add(new_prediction)
			db.session.commit()
		else:
			raise ValueError(f'{_sensor} is not currently supported')

	def get_latest_predictions(_timestamp_offset):
		subq = db.session.query(CMSAPrediction.sensor, CMSAPrediction.timestamp_offset, func.max(CMSAPrediction.timestamp).label('maxtsp')).group_by(CMSAPrediction.sensor, CMSAPrediction.timestamp_offset).filter(CMSAPrediction.timestamp_offset==_timestamp_offset).subquery('t2')
		return [CMSAPrediction.json(reading) for reading in db.session.query(CMSAPrediction).filter(CMSAPrediction.timestamp_offset==_timestamp_offset).join(subq, and_(CMSAPrediction.sensor==subq.c.sensor, CMSAPrediction.timestamp==subq.c.maxtsp))]

	def __repr__(self):
		return json.dumps(self.json())

from statsmodels import api as sm_api

import numpy as np
import pandas as pd


def predict(df, offset):
	y = df['total_count']
	prediction_point = len(y)+offset
	predictor = sm_api.tsa.AutoReg(y, lags=24, seasonal=True, period=24).fit()
	return predictor.predict(start=prediction_point, end=prediction_point)
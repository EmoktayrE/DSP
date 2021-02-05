from statsmodels import api as sm_api

import numpy as np
import pandas as pd


def predict_column_at(df, column, offset):
	y = df[column]
	prediction_point = len(y)+offset
	predictor = sm_api.tsa.AutoReg(y, lags = 20).fit()
	return predictor.predict(start=prediction_point, end=prediction_point)
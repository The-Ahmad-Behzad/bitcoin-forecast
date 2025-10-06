# backend/models/arima_model.py
import warnings
import numpy as np
from statsmodels.tsa.arima.model import ARIMA
from .base_model import BaseModel

class ARIMAModel(BaseModel):
    def __init__(self, order=(2, 1, 2)):
        self.order = order
        self.model_fit = None

    def fit(self, series):
        warnings.filterwarnings("ignore")
        model = ARIMA(series, order=self.order)
        self.model_fit = model.fit()
        return self

    def predict(self, steps=1):
        if self.model_fit is None:
            raise ValueError("Model not fitted yet")
        forecast = self.model_fit.forecast(steps=steps)
        return np.array(forecast)

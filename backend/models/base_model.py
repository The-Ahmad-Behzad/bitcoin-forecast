# # backend/models/base_model.py
# from abc import ABC, abstractmethod

# class BaseModel(ABC):
#     """Abstract base forecaster defining common API."""

#     @abstractmethod
#     def fit(self, series):
#         """Train or fit model on given time series"""
#         pass

#     @abstractmethod
#     def predict(self, steps):
#         """Forecast given number of future steps"""
#         pass

#     @abstractmethod
#     def evaluate(self, true, pred):
#         """Return RMSE, MAE, MAPE"""
#         from sklearn.metrics import mean_squared_error, mean_absolute_error
#         import numpy as np

#         rmse = mean_squared_error(true, pred, squared=False)
#         mae = mean_absolute_error(true, pred)
#         mape = np.mean(np.abs((true - pred) / true)) * 100
#         return {"rmse": rmse, "mae": mae, "mape": mape}

# backend/models/base_model.py
from abc import ABC, abstractmethod
from sklearn.metrics import mean_absolute_error, root_mean_squared_error
import numpy as np

class BaseModel(ABC):
    """Abstract base forecaster defining common API."""

    @abstractmethod
    def fit(self, series):
        """Train or fit model on given time series"""
        pass

    @abstractmethod
    def predict(self, steps):
        """Forecast given number of future steps"""
        pass

    def evaluate(self, true, pred):
        """Return RMSE, MAE, MAPE"""
        rmse = root_mean_squared_error(true, pred)
        mae = mean_absolute_error(true, pred)
        mape = np.mean(np.abs((true - pred) / true)) * 100
        return {"rmse": rmse, "mae": mae, "mape": mape}

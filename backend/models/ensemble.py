# backend/models/ensemble.py
import numpy as np

def combine_predictions(ma_pred, arima_pred, gru_pred=None):
    if gru_pred is not None:
        ensemble = 0.5 * arima_pred + 0.5 * ma_pred
        # ensemble = 0.4 * arima_pred + 0.4 * ma_pred + 0.2 * gru_pred
    else:
        ensemble = 0.5 * arima_pred + 0.5 * ma_pred
    return np.array(ensemble)

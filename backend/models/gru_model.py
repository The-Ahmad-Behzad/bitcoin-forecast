# backend/models/gru_model.py
try:
    import tensorflow as tf
    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import GRU, Dense
    from tensorflow.keras.optimizers import Adam
    import numpy as np
    from .base_model import BaseModel
except ImportError:
    raise ImportError("TensorFlow not available for GRUForecaster")

class GRUForecaster(BaseModel):
    def __init__(self, lookback=10, epochs=5):
        self.lookback = lookback
        self.epochs = epochs
        self.model = None
        self.last_seq = None

    def _prepare_data(self, series):
        X, y = [], []
        for i in range(len(series) - self.lookback):
            X.append(series[i:i+self.lookback])
            y.append(series[i+self.lookback])
        return np.array(X)[..., np.newaxis], np.array(y)

    def fit(self, series):
        X, y = self._prepare_data(np.array(series))
        self.model = Sequential([
            GRU(32, input_shape=(self.lookback, 1)),
            Dense(1)
        ])
        self.model.compile(optimizer=Adam(0.01), loss="mse")
        self.model.fit(X, y, epochs=self.epochs, verbose=0)
        self.last_seq = series[-self.lookback:]
        return self

    def predict(self, steps=1):
        seq = np.array(self.last_seq, dtype=np.float32)
        preds = []
        for _ in range(steps):
            x = seq[-self.lookback:].reshape(1, self.lookback, 1)
            pred = self.model.predict(x, verbose=0)[0, 0]
            preds.append(pred)
            seq = np.append(seq, pred)
        return np.array(preds)

# backend/models/moving_average.py
import numpy as np
from .base_model import BaseModel

class MovingAverageModel(BaseModel):
    def __init__(self, window=5):
        self.window = window
        self.history = None

    def fit(self, series):
        self.history = np.array(series)
        return self

    def predict(self, steps=1):
        if self.history is None:
            raise ValueError("Model not fitted yet")
        preds = []
        hist = list(self.history)
        for _ in range(steps):
            pred = np.mean(hist[-self.window:])
            preds.append(pred)
            hist.append(pred)
        return np.array(preds)

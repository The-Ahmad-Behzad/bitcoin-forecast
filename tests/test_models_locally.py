from backend.models.moving_average import MovingAverageModel
from backend.models.arima_model import ARIMAModel
import numpy as np

series = np.random.randn(30).cumsum() + 100
ma = MovingAverageModel(window=5).fit(series)
arima = ARIMAModel().fit(series)

print("MA:", ma.predict(3))
print("ARIMA:", arima.predict(3))

import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import numpy as np

try:
    from backend.models.gru_model import GRUForecaster
except ImportError as e:
    print("❌ GRUForecaster import failed:", e)
    print("Make sure TensorFlow is installed: pip install tensorflow")
    exit()

# --- Synthetic BTC-style price data ---
np.random.seed(42)
series = np.cumsum(np.random.randn(100)) + 100  # trending time series

# --- Initialize and fit model ---
model = GRUForecaster(lookback=10, epochs=5)
print("✅ Fitting GRU model...")
model.fit(series)

# --- Predict next 5 future steps ---
pred = model.predict(steps=5)
print("✅ Predictions (5-step):", pred)

# --- Evaluate with fake true values (for test only) ---
true_values = series[-5:] + np.random.randn(5) * 0.5
metrics = model.evaluate(true_values, pred[-5:])
print("📊 Evaluation metrics:", metrics)


// // frontend/src/components/CandlestickChart.jsx
// import React from "react";
// import Plot from "react-plotly.js";

// export default function CandlestickChart({ data, forecast }) {
//   if (!data || data.length === 0) {
//     return <p className="text-center text-gray-500">No data available.</p>;
//   }

//   const traceHist = {
//     x: data.map((d) => new Date(d.date)),
//     open: data.map((d) => d.open),
//     high: data.map((d) => d.high),
//     low: data.map((d) => d.low),
//     close: data.map((d) => d.close),
//     type: "candlestick",
//     name: "BTC-USD",
//     increasing: { line: { color: "green" } },
//     decreasing: { line: { color: "red" } },
//   };

//   const traces = [traceHist];

//   if (forecast) {
//     const lastDate = new Date(data[data.length - 1].date);
//     const forecastDates = Array.from(
//       { length: forecast.ensemble.length },
//       (_, i) => new Date(lastDate.getTime() + (i + 1) * 3600 * 1000)
//     );

//     // Overlay forecasts
//     if (forecast.moving_average)
//       traces.push({
//         x: forecastDates,
//         y: forecast.moving_average,
//         mode: "lines",
//         name: "Moving Average Forecast",
//         line: { color: "gray", dash: "dot" },
//       });
//     if (forecast.arima)
//       traces.push({
//         x: forecastDates,
//         y: forecast.arima,
//         mode: "lines",
//         name: "ARIMA Forecast",
//         line: { color: "blue" },
//       });
//     if (forecast.gru)
//       traces.push({
//         x: forecastDates,
//         y: forecast.gru,
//         mode: "lines",
//         name: "GRU Forecast",
//         line: { color: "green" },
//       });
//     if (forecast.ensemble)
//       traces.push({
//         x: forecastDates,
//         y: forecast.ensemble,
//         mode: "lines",
//         name: "Ensemble Forecast",
//         line: { color: "orange", dash: "dashdot", width: 3 },
//       });
//   }

//   const layout = {
//     title: "Bitcoin (BTC-USD) — Historical + Forecast",
//     xaxis: { title: "Date" },
//     yaxis: { title: "Price (USD)" },
//     autosize: true,
//     plot_bgcolor: "#fafafa",
//     paper_bgcolor: "#fafafa",
//   };

//   return (
//     <Plot
//       data={traces}
//       layout={layout}
//       style={{ width: "100%", height: "600px" }}
//       config={{ responsive: true }}
//     />
//   );
// }


import React from "react";
import Plot from "react-plotly.js";

export default function CandlestickChart({ data, forecast }) {
  if (!data || data.length === 0) {
    return <p className="text-center text-gray-500">No data available.</p>;
  }

  // --- Historical candlestick trace ---
  const traceHist = {
    x: data.map((d) => new Date(d.date)),
    open: data.map((d) => d.open),
    high: data.map((d) => d.high),
    low: data.map((d) => d.low),
    close: data.map((d) => d.close),
    type: "candlestick",
    name: "BTC-USD (Historical)",
    increasing: { line: { color: "green" } },
    decreasing: { line: { color: "red" } },
  };

  const traces = [traceHist];

  // --- Forecast overlays ---
  if (forecast && (forecast.ensemble || forecast.arima || forecast.moving_average)) {
    // ✅ If backend provided exact forecast dates, use them
    let forecastDates = [];
    if (forecast.dates && forecast.dates.length > 0) {
      forecastDates = forecast.dates.map((d) => new Date(d));
    } else {
      // 🧭 Otherwise, generate dates sequentially after last observed date
      const lastDate = new Date(data[data.length - 1].date);
      forecastDates = Array.from(
        { length: forecast.ensemble?.length || 0 },
        (_, i) => new Date(lastDate.getTime() + (i + 1) * 24 * 3600 * 1000) // 24h per step
      );
    }

    // --- Add model forecast traces ---
    if (forecast.moving_average) {
      traces.push({
        x: forecastDates,
        y: forecast.moving_average,
        mode: "lines",
        name: "Moving Average Forecast",
        line: { color: "gray", dash: "dot", width: 2 },
      });
    }

    if (forecast.arima) {
      traces.push({
        x: forecastDates,
        y: forecast.arima,
        mode: "lines",
        name: "ARIMA Forecast",
        line: { color: "blue", width: 2 },
      });
    }

    if (forecast.gru) {
      traces.push({
        x: forecastDates,
        y: forecast.arima,
        mode: "lines",
        name: "GRU Forecast",
        line: { color: "pink", dash:"dashdot", width: 2 },
      });
    }

    if (forecast.ensemble) {
      traces.push({
        x: forecastDates,
        y: forecast.ensemble,
        mode: "lines",
        name: "Ensemble Forecast",
        line: { color: "orange", dash: "dashdot", width: 3 },
      });
    }
  }

  const layout = {
    title: "Bitcoin (BTC-USD) — Historical + Forecast",
    xaxis: { title: "Date" },
    yaxis: { title: "Price (USD)" },
    autosize: true,
    plot_bgcolor: "#fafafa",
    paper_bgcolor: "#fafafa",
    legend: { orientation: "h", x: 0.3, y: -0.2 },
  };

  return (
    <Plot
      data={traces}
      layout={layout}
      style={{ width: "100%", height: "600px" }}
      config={{ responsive: true }}
    />
  );
}

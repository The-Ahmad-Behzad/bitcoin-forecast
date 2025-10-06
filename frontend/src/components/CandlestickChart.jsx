import React from "react";
import Plot from "react-plotly.js";

export default function CandlestickChart({ data }) {
  if (!data || data.length === 0) {
    return <p className="text-center text-gray-500">No data available.</p>;
  }

  const trace = {
    x: data.map((d) => new Date(d.date)),
    open: data.map((d) => d.open),
    high: data.map((d) => d.high),
    low: data.map((d) => d.low),
    close: data.map((d) => d.close),
    type: "candlestick",
    name: "BTC-USD",
    increasing: { line: { color: "green" } },
    decreasing: { line: { color: "red" } },
  };

  const layout = {
    title: "Bitcoin (BTC-USD) Price Chart",
    xaxis: { title: "Date" },
    yaxis: { title: "Price (USD)" },
    autosize: true,
    plot_bgcolor: "#fafafa",
    paper_bgcolor: "#fafafa",
  };

  return (
    <Plot
      data={[trace]}
      layout={layout}
      style={{ width: "100%", height: "600px" }}
      config={{ responsive: true }}
    />
  );
}

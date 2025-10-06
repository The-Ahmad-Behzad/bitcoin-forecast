import axios from "axios";

const API_BASE = "/api";

export const pingServer = async () => {
  const res = await axios.get(`${API_BASE}/ping`);
  return res.data;
};

export const getHistoricalData = async (limit = 15) => {
  const res = await axios.get(`${API_BASE}/historical?limit=${limit}`);
  return res.data;
};

export const triggerIngestion = async (start, end) => {
  const res = await axios.post(`${API_BASE}/ingest`, { start, end });
  return res.data;
};

// // src/App.jsx
// import React, { useEffect, useState } from "react";
// import { getHistoricalData, triggerIngestion, pingServer } from "./services/api";
// import CandlestickChart from "./components/CandlestickChart";
// import HorizonSelector from "./components/HorizonSelector";

// export default function App() {
//   const [data, setData] = useState([]);
//   const [horizon, setHorizon] = useState(24);
//   const [status, setStatus] = useState("Loading...");
//   const [loading, setLoading] = useState(false);

//   useEffect(() => {
//     async function init() {
//       try {
//         const ping = await pingServer();
//         if (ping.status === "ok") {
//           const res = await getHistoricalData(20);
//           setData(res.data);
//           setStatus("Ready");
//         }
//       } catch (err) {
//         setStatus("Backend not reachable");
//       }
//     }
//     init();
//   }, []);

//   const handleRefresh = async () => {
//     setLoading(true);
//     try {
//       const today = new Date().toISOString().split("T")[0];
//       const res = await triggerIngestion("2025-09-01", today);
//       console.log(res);
//       const newData = await getHistoricalData(20);
//       setData(newData.data);
//     } catch (err) {
//       alert("Error triggering ingestion");
//     } finally {
//       setLoading(false);
//     }
//   };

//   return (
//     <div className="min-h-screen bg-gray-50 p-6">
//       <h1 className="text-3xl font-bold text-center mb-6">
//         ₿ Bitcoin Forecast Dashboard
//       </h1>

//       <div className="flex justify-center gap-4 mb-6">
//         <button
//           onClick={handleRefresh}
//           disabled={loading}
//           className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700"
//         >
//           {loading ? "Updating..." : "Refresh Data"}
//         </button>
//       </div>

//       <HorizonSelector selected={horizon} onChange={setHorizon} />
//       <CandlestickChart data={data} />

//       <p className="text-center text-gray-500 mt-6">Status: {status}</p>
//     </div>
//   );
// }

// src/App.jsx
import React, { useEffect, useState } from "react";
import { getHistoricalData, triggerIngestion, pingServer } from "./services/api";
import CandlestickChart from "./components/CandlestickChart";
import HorizonSelector from "./components/HorizonSelector";

export default function App() {
  const [data, setData] = useState([]);
  const [horizon, setHorizon] = useState(24);
  const [status, setStatus] = useState("Checking backend...");
  const [statusColor, setStatusColor] = useState("text-gray-500");
  const [loading, setLoading] = useState(false);

  // 🔁 Check backend health periodically
  useEffect(() => {
    const checkBackend = async () => {
      try {
        const ping = await pingServer();
        if (ping && ping.status === "ok") {
          setStatus("🟢 Backend Connected");
          setStatusColor("text-green-600");
        } else {
          setStatus("🔴 Backend not reachable");
          setStatusColor("text-red-600");
        }
      } catch (err) {
        setStatus("🔴 Backend not reachable");
        setStatusColor("text-red-600");
      }
    };

    checkBackend();
    const interval = setInterval(checkBackend, 10000); // recheck every 10s
    return () => clearInterval(interval);
  }, []);

  // 🔽 Fetch initial data when backend is available
  useEffect(() => {
    async function init() {
      try {
        const ping = await pingServer();
        if (ping.status === "ok") {
          const res = await getHistoricalData(20);
          setData(res.data || res);
          setStatus("🟢 Backend Connected");
          setStatusColor("text-green-600");
        } else {
          setStatus("🔴 Backend not reachable");
          setStatusColor("text-red-600");
        }
      } catch (err) {
        setStatus("🔴 Backend not reachable");
        setStatusColor("text-red-600");
      }
    }
    init();
  }, []);

  const handleRefresh = async () => {
    setLoading(true);
    try {
      const today = new Date().toISOString().split("T")[0];
      const res = await triggerIngestion("2025-09-01", today);
      console.log("Ingestion triggered:", res);
      const newData = await getHistoricalData(20);
      setData(newData.data || newData);
    } catch (err) {
      alert("Error triggering ingestion");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <h1 className="text-3xl font-bold text-center mb-6">
        ₿ Bitcoin Forecast Dashboard
      </h1>

      <div className="flex justify-center gap-4 mb-6">
        <button
          onClick={handleRefresh}
          disabled={loading}
          className={`${
            loading ? "bg-gray-400" : "bg-blue-600 hover:bg-blue-700"
          } text-white px-4 py-2 rounded-lg transition`}
        >
          {loading ? "Updating..." : "Refresh Data"}
        </button>
      </div>

      <HorizonSelector selected={horizon} onChange={setHorizon} />
      <CandlestickChart data={data} />

      <p className={`text-center font-semibold mt-6 ${statusColor}`}>
        Status: {status}
      </p>
    </div>
  );
}

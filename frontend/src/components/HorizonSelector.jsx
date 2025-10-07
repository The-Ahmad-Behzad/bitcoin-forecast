import React from "react";

const options = [
  { label: "5 Hours", value: 5 },
  { label: "10 Hours", value: 10 },
  { label: "20 Hours", value: 20 },
  { label: "30 Hours", value: 30 },
];

export default function HorizonSelector({ selected, onChange }) {
  return (
    <div className="flex gap-2 justify-center mb-4">
      {options.map((opt) => (
        <button
          key={opt.value}
          onClick={() => onChange(opt.value)}
          className={`px-4 py-2 rounded-lg font-medium transition ${
            selected === opt.value
              ? "bg-blue-600 text-white"
              : "bg-gray-200 hover:bg-blue-100"
          }`}
        >
          {opt.label}
        </button>
      ))}
    </div>
  );
}

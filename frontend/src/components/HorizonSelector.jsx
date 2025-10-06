import React from "react";

const options = [
  { label: "1 Hour", value: 1 },
  { label: "3 Hours", value: 3 },
  { label: "24 Hours", value: 24 },
  { label: "72 Hours", value: 72 },
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

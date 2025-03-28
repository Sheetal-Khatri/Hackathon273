import React, { useState } from "react";
import { ToastContainer, toast } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";

// List of reservoirs and their CDEC IDs
const reservoirList = [
  { name: "Shasta Lake", id: "SHA" },
  { name: "Lake Oroville", id: "ORO" },
  { name: "Trinity Lake", id: "CLE" },
  { name: "New Melones Lake", id: "NML" },
  { name: "San Luis Reservoir", id: "SNL" },
  { name: "Don Pedro Reservoir", id: "DNP" },
  { name: "Lake Berryessa", id: "BER" },
  { name: "Folsom Lake", id: "FOL" },
  { name: "New Bullards Bar Reservoir", id: "BUL" },
  { name: "Pine Flat Lake", id: "PNF" },
];

const DEFAULT_START_DATE = "2025-01-01";
const DEFAULT_END_DATE = "2025-12-31";

function AdminConsole() {
  const [configs, setConfigs] = useState(() =>
    reservoirList.map((res) => ({
      name: res.name,
      cdecId: res.id,
      startDate: DEFAULT_START_DATE,
      endDate: DEFAULT_END_DATE,
    }))
  );

  // Handle changes to startDate or endDate for each reservoir
  const handleDateChange = (index, field, value) => {
    const updatedConfigs = [...configs];
    updatedConfigs[index][field] =
      value.trim() === ""
        ? field === "startDate"
          ? DEFAULT_START_DATE
          : DEFAULT_END_DATE
        : value;
    setConfigs(updatedConfigs);
  };

  // Submit configurations by calling POST /api/configs
  const handleSubmit = (event) => {
    event.preventDefault();

    fetch("http://localhost:5000/api/configs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(configs),
    })
      .then((response) => {
        if (!response.ok) {
          throw new Error("Network response was not OK");
        }
        return response.json();
      })
      .then((data) => {
        console.log("Server response:", data);
        toast.success("Configurations saved successfully!");
      })
      .catch((error) => {
        console.error("Error updating configs:", error);
        toast.error("Failed to save configurations.");
      });
  };

  // Sync by calling GET /api/run
  const handleSync = () => {
    fetch("http://localhost:5000/api/run")
      .then((response) => {
        if (!response.ok) {
          throw new Error("Sync failed");
        }
        return response.json();
      })
      .then((data) => {
        console.log("Sync response:", data);
        toast.success("Sync triggered successfully!");
      })
      .catch((error) => {
        console.error("Error syncing data:", error);
        toast.error("Failed to sync data.");
      });
  };

  return (
    <div style={styles.container}>
      <h2 style={styles.heading}>Reservoir Configuration</h2>
      <form onSubmit={handleSubmit}>
        <table style={styles.table}>
          <thead>
            <tr>
              <th style={styles.th}>Reservoir Name</th>
              <th style={styles.th}>CDEC ID</th>
              <th style={styles.th}>Start Date</th>
              <th style={styles.th}>End Date</th>
            </tr>
          </thead>
          <tbody>
            {configs.map((cfg, index) => (
              <tr key={cfg.cdecId} style={styles.tr}>
                <td style={styles.td}>{cfg.name}</td>
                <td style={styles.td}>{cfg.cdecId}</td>
                <td style={styles.td}>
                  <input
                    type="date"
                    value={cfg.startDate}
                    onChange={(e) =>
                      handleDateChange(index, "startDate", e.target.value)
                    }
                    style={styles.input}
                  />
                </td>
                <td style={styles.td}>
                  <input
                    type="date"
                    value={cfg.endDate}
                    onChange={(e) =>
                      handleDateChange(index, "endDate", e.target.value)
                    }
                    style={styles.input}
                  />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        <div style={styles.buttonContainer}>
          <button type="submit" style={styles.button}>
            Submit Configurations
          </button>
          <button
            type="button"
            onClick={handleSync}
            style={{ ...styles.button, marginLeft: "1rem" }}
          >
            Sync
          </button>
        </div>
      </form>
      {/* Toast Container renders the toast messages */}
      <ToastContainer position="top-center" autoClose={3000} />
    </div>
  );
}

const styles = {
  container: {
    margin: "2rem auto",
    maxWidth: "800px",
    fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
    background: "#f9f9f9",
    padding: "2rem",
    borderRadius: "8px",
    boxShadow: "0 2px 8px rgba(0,0,0,0.1)",
  },
  heading: {
    textAlign: "center",
    marginBottom: "1.5rem",
    color: "#333",
  },
  table: {
    width: "100%",
    borderCollapse: "collapse",
    marginBottom: "1.5rem",
  },
  th: {
    borderBottom: "2px solid #ddd",
    padding: "0.75rem",
    textAlign: "left",
    background: "#f1f1f1",
    color: "#333",
  },
  tr: {
    borderBottom: "1px solid #eee",
  },
  td: {
    padding: "0.75rem",
  },
  input: {
    padding: "0.5rem",
    border: "1px solid #ccc",
    borderRadius: "4px",
    width: "100%",
  },
  buttonContainer: {
    textAlign: "center",
  },
  button: {
    padding: "0.75rem 1.5rem",
    fontSize: "1rem",
    color: "#fff",
    background: "#007BFF",
    border: "none",
    borderRadius: "4px",
    cursor: "pointer",
    boxShadow: "0 2px 4px rgba(0, 123, 255, 0.4)",
  },
};

export default AdminConsole;

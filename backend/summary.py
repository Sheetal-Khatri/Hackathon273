from flask import Flask, jsonify
import mysql.connector
import matplotlib.pyplot as plt
import sys
import math

app = Flask(__name__)   

# MySQL connection configuration
MYSQL_CONFIG = {
    "host": "reservoir-db.c1sk2imgwsr1.us-west-1.rds.amazonaws.com",
    "user": "admin",
    "password": "hackathoncmpe273",
    "database": "reservoir"
}

def get_summary_data():
    """
    Connects to the MySQL database, reads from the filtered_data table,
    and calculates the max, min, and average value of the feet field grouped by cdec_id.
    Returns a list of dictionaries with the summary data.
    """
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        query = """
            SELECT cdec_id, MAX(feet) AS max_feet, MIN(feet) AS min_feet, AVG(feet) AS avg_feet
            FROM filtered_data
            GROUP BY cdec_id;
        """
        cursor.execute(query)
        rows = cursor.fetchall()
    except mysql.connector.Error as e:
        print("Error reading data from MySQL", e)
        rows = []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    summary_list = []
    for row in rows:
        summary_list.append({
            "cdec_id": row[0],
            "max_feet": float(row[1]),
            "min_feet": float(row[2]),
            "avg_feet": float(row[3])
        })
    return summary_list

@app.route("/api/summary", methods=["GET"])
def summary_api():
    """
    API endpoint that returns the summary statistics for each reservoir as JSON.
    """
    data = get_summary_data()
    return jsonify(data)

def plot_aggregated_summary():
    """
    Aggregates summary graphs for each reservoir into a single image using subplots.
    Each subplot shows the max, min, and average feet values.
    The aggregated graph is saved as 'aggregated_summary.png'.
    """
    data = get_summary_data()
    if not data:
        print("No data available to plot.")
        return

    n = len(data)
    # Choose layout: 2 columns, rows based on the number of reservoirs
    cols = 2
    rows = math.ceil(n / cols)
    fig, axs = plt.subplots(rows, cols, figsize=(8 * cols, 6 * rows))
    
    # In case there is only one subplot, ensure axs is iterable
    if n == 1:
        axs = [axs]
    else:
        axs = axs.flatten()

    for i, record in enumerate(data):
        cdec_id = record["cdec_id"]
        metrics = ["Max", "Min", "Avg"]
        values = [record["max_feet"], record["min_feet"], record["avg_feet"]]
        axs[i].bar(metrics, values, color=['blue', 'green', 'orange'])
        axs[i].set_title(f"Reservoir {cdec_id}")
        axs[i].set_xlabel("Metric")
        axs[i].set_ylabel("Feet")
        axs[i].set_ylim(min(values) - 1, max(values) + 1)
    
    # Remove any unused subplots if the total number isn't an exact multiple of cols
    for j in range(i + 1, len(axs)):
        fig.delaxes(axs[j])
        
    plt.tight_layout()
    filename = "aggregated_summary.png"
    plt.savefig(filename)
    print(f"Aggregated summary graph saved as {filename}")
    plt.close()

if __name__ == "__main__":
    # If the script is run with the argument "plot", generate the graphs.
    # Otherwise, run the Flask API.
    if len(sys.argv) > 1 and sys.argv[1] == "plot":
        plot_aggregated_summary()
    else:
        app.run(debug=True, port=5002)
# --- Imports Block ---
from datetime import datetime
import streamlit as st
import duckdb
import pandas as pd
import pyarrow as pa  # Import pyarrow

# To suppress scientific notations
pd.set_option("display.float_format", lambda x: "%.3f" % x)

# --- Setup Block ---
# Initialize DuckDB connection
con = duckdb.connect(database=':memory:', read_only=False)

# Local CSV file path (update this path to the location of your NYC_Energy dataset)
local_path = "/Users/nickscavuzzo/Downloads/predictoortwo (5).csv"

# Read the CSV file into a PyArrow table using DuckDB
arrow_table = con.execute(
    f"SELECT *, CAST(STRPTIME(Timestamp, '%m/%d/%Y %H:%M') AS DATE) AS FormattedTimestamp FROM read_csv_auto('{local_path}')"
).arrow()


# Define a name for the table in DuckDB
DuckDBTableName = "Predictoor"

# Register the PyArrow table with DuckDB
con.register(DuckDBTableName, arrow_table)
print(f"Registered table {DuckDBTableName} successfully.")

# List of table names and corresponding SQL queries for each table
table_names = ["accuracy_predictions", "total_profit"]

# Define a dictionary with table names as keys and their corresponding SQL creation queries as values
sql_queries = {
    "accuracy_predictions": """
        CREATE TABLE accuracy_predictions AS
        SELECT
            TradingPair,
            COUNT(*) FILTER (WHERE PredictedValue = TRUE) AS up_predictions,
            COUNT(*) FILTER (WHERE PredictedValue = FALSE) AS down_predictions,
            COUNT(*) AS total_predictions,
            COUNT(*) FILTER (WHERE PredictedValue = TRUE AND TrueValue = TRUE) AS up_correct,
            COUNT(*) FILTER (WHERE PredictedValue = FALSE AND TrueValue = FALSE) AS down_correct,
            COUNT(*) FILTER (WHERE PredictedValue = TrueValue) AS total_correct,
            CASE WHEN COUNT(*) FILTER (WHERE PredictedValue = TRUE) = 0 THEN NULL ELSE 
                COUNT(*) FILTER (WHERE PredictedValue = TRUE AND TrueValue = TRUE) * 100.0 / COUNT(*) FILTER (WHERE PredictedValue = TRUE) END AS up_accuracy,
            CASE WHEN COUNT(*) FILTER (WHERE PredictedValue = FALSE) = 0 THEN NULL ELSE 
                COUNT(*) FILTER (WHERE PredictedValue = FALSE AND TrueValue = FALSE) * 100.0 / COUNT(*) FILTER (WHERE PredictedValue = FALSE) END AS down_accuracy,
            CASE WHEN COUNT(*) = 0 THEN NULL ELSE 
                COUNT(*) FILTER (WHERE PredictedValue = TrueValue) * 100.0 / COUNT(*) END AS total_accuracy
        FROM Predictoor
        GROUP BY TradingPair;
            """,
    "total_profit": """
        CREATE TABLE total_profit AS
        SELECT
            TradingPair,
            SUM(Stake) AS total_stake,
            SUM(Payout) AS total_payout,
            SUM(Payout) - SUM(Stake) AS net_profit
        FROM predictoor
        GROUP BY TradingPair
        UNION ALL
        SELECT
            'TOTAL' AS TradingPair,
            SUM(Stake) AS total_stake,
            SUM(Payout) AS total_payout,
            SUM(Payout) - SUM(Stake) AS net_profit
        FROM predictoor;
    """,
}

# Create and register downstream data tables in DuckDB
for table_name, sql in sql_queries.items():
    try:
        # Execute the SQL query to create the table
        con.execute(sql)
        print(
            f"Table '{table_name}' has been created and registered successfully.")
    except Exception as e:
        print(f"Error creating table '{table_name}': {e}")


# Streamlit app start
st.title('Predictoor Stats')

# Separate date pickers for start and end dates
start_date = st.sidebar.date_input(
    "Select Start Date", value=datetime.now().date(), key='start_date')
end_date = st.sidebar.date_input(
    "Select End Date", value=datetime.now().date(), key='end_date')

# Format dates to match your dataset's format
formatted_start_date = start_date.strftime('%Y-%m-%d')
formatted_end_date = end_date.strftime('%Y-%m-%d')

# Retrieve the table names directly from DuckDB after they have been created
table_names = con.execute("SHOW TABLES").fetchall()
table_names = [name[0] for name in table_names]

# Dropdown to select between the tables
selected_table = st.selectbox(
    'Select a table to view', table_names, key='table_select')

# Display table
if selected_table:
    if selected_table == 'Predictoor':
        # Add filters for the Predictoor table
        column_names = con.execute(f"PRAGMA table_info({selected_table})").fetchdf()[
            'name'].tolist()
        filters = {}
        for col in column_names:
            if col in ['TradingPair', 'TimeFrame', 'Exchange']:  # Adjust according to data types
                filters[col] = st.sidebar.selectbox(f"Filter by {col}", ['', 'ALL'] + list(
                    con.execute(f"SELECT DISTINCT {col} FROM {selected_table}").fetchdf()[col]))
            elif col in ['PredictedValue', 'TrueValue']:  # For boolean columns
                filters[col] = st.sidebar.selectbox(
                    f"Filter by {col}", ['ALL', True, False])

        # Construct the WHERE clause based on filters
        where_clauses = []
        for col, value in filters.items():
            if value and value != 'ALL':
                if isinstance(value, str):
                    where_clauses.append(f"{col} = '{value}'")
                else:
                    where_clauses.append(f"{col} = {value}")

        where_clause = " AND ".join(where_clauses)
        if where_clause:
            where_clause = "WHERE " + where_clause

        # Define the query for the Predictoor table
        query = f"SELECT * FROM {selected_table} {where_clause}"
        df = con.execute(query).fetchdf()
        st.write(df)

    elif selected_table == 'accuracy_predictions':
        # Updated query for the accuracy_predictions table
        query = f"""
        SELECT
            TradingPair,
            TimeFrame,
            COUNT(*) FILTER (WHERE PredictedValue = TRUE) AS up_predictions,
            COUNT(*) FILTER (WHERE PredictedValue = FALSE) AS down_predictions,
            COUNT(*) AS total_predictions,
            COUNT(*) FILTER (WHERE PredictedValue = TRUE AND TrueValue = TRUE) AS up_correct,
            COUNT(*) FILTER (WHERE PredictedValue = FALSE AND TrueValue = FALSE) AS down_correct,
            COUNT(*) FILTER (WHERE PredictedValue = TrueValue) AS total_correct,
            CASE WHEN COUNT(*) FILTER (WHERE PredictedValue = TRUE) = 0 THEN NULL ELSE 
                COUNT(*) FILTER (WHERE PredictedValue = TRUE AND TrueValue = TRUE) * 100.0 / COUNT(*) FILTER (WHERE PredictedValue = TRUE) END AS up_accuracy,
            CASE WHEN COUNT(*) FILTER (WHERE PredictedValue = FALSE) = 0 THEN NULL ELSE 
                COUNT(*) FILTER (WHERE PredictedValue = FALSE AND TrueValue = FALSE) * 100.0 / COUNT(*) FILTER (WHERE PredictedValue = FALSE) END AS down_accuracy,
            CASE WHEN COUNT(*) = 0 THEN NULL ELSE 
                COUNT(*) FILTER (WHERE PredictedValue = TrueValue) * 100.0 / COUNT(*) END AS total_accuracy
        FROM Predictoor
        WHERE CAST(STRPTIME(Timestamp, '%m/%d/%Y %H:%M') AS DATE) BETWEEN '{formatted_start_date}' AND '{formatted_end_date}'
        GROUP BY TradingPair, TimeFrame

        UNION ALL

        SELECT
            TradingPair,
            '1hr' AS TimeFrame,
            COUNT(*) FILTER (WHERE PredictedValue = TRUE) AS up_predictions,
            COUNT(*) FILTER (WHERE PredictedValue = FALSE) AS down_predictions,
            COUNT(*) AS total_predictions,
            COUNT(*) FILTER (WHERE PredictedValue = TRUE AND TrueValue = TRUE) AS up_correct,
            COUNT(*) FILTER (WHERE PredictedValue = FALSE AND TrueValue = FALSE) AS down_correct,
            COUNT(*) FILTER (WHERE PredictedValue = TrueValue) AS total_correct,
            CASE WHEN COUNT(*) FILTER (WHERE PredictedValue = TRUE) = 0 THEN NULL ELSE 
                COUNT(*) FILTER (WHERE PredictedValue = TRUE AND TrueValue = TRUE) * 100.0 / COUNT(*) FILTER (WHERE PredictedValue = TRUE) END AS up_accuracy,
            CASE WHEN COUNT(*) FILTER (WHERE PredictedValue = FALSE) = 0 THEN NULL ELSE 
                COUNT(*) FILTER (WHERE PredictedValue = FALSE AND TrueValue = FALSE) * 100.0 / COUNT(*) FILTER (WHERE PredictedValue = FALSE) END AS down_accuracy,
            CASE WHEN COUNT(*) = 0 THEN NULL ELSE 
                COUNT(*) FILTER (WHERE PredictedValue = TrueValue) * 100.0 / COUNT(*) END AS total_accuracy
        FROM Predictoor
        WHERE TimeFrame = '1hr' AND CAST(STRPTIME(Timestamp, '%m/%d/%Y %H:%M') AS DATE) BETWEEN '{formatted_start_date}' AND '{formatted_end_date}'
        GROUP BY TradingPair

        UNION ALL

        SELECT
            TradingPair,
            '5min' AS TimeFrame,
            COUNT(*) FILTER (WHERE PredictedValue = TRUE) AS up_predictions,
            COUNT(*) FILTER (WHERE PredictedValue = FALSE) AS down_predictions,
            COUNT(*) AS total_predictions,
            COUNT(*) FILTER (WHERE PredictedValue = TRUE AND TrueValue = TRUE) AS up_correct,
            COUNT(*) FILTER (WHERE PredictedValue = FALSE AND TrueValue = FALSE) AS down_correct,
            COUNT(*) FILTER (WHERE PredictedValue = TrueValue) AS total_correct,
            CASE WHEN COUNT(*) FILTER (WHERE PredictedValue = TRUE) = 0 THEN NULL ELSE 
                COUNT(*) FILTER (WHERE PredictedValue = TRUE AND TrueValue = TRUE) * 100.0 / COUNT(*) FILTER (WHERE PredictedValue = TRUE) END AS up_accuracy,
            CASE WHEN COUNT(*) FILTER (WHERE PredictedValue = FALSE) = 0 THEN NULL ELSE 
                COUNT(*) FILTER (WHERE PredictedValue = FALSE AND TrueValue = FALSE) * 100.0 / COUNT(*) FILTER (WHERE PredictedValue = FALSE) END AS down_accuracy,
            CASE WHEN COUNT(*) = 0 THEN NULL ELSE 
                COUNT(*) FILTER (WHERE PredictedValue = TrueValue) * 100.0 / COUNT(*) END AS total_accuracy
        FROM Predictoor
        WHERE TimeFrame = '5min' AND CAST(STRPTIME(Timestamp, '%m/%d/%Y %H:%M') AS DATE) BETWEEN '{formatted_start_date}' AND '{formatted_end_date}'
        GROUP BY TradingPair

        UNION ALL

        SELECT
            TradingPair,
            NULL AS TimeFrame,
            COUNT(*) FILTER (WHERE PredictedValue = TRUE) AS up_predictions,
            COUNT(*) FILTER (WHERE PredictedValue = FALSE) AS down_predictions,
            COUNT(*) AS total_predictions,
            COUNT(*) FILTER (WHERE PredictedValue = TRUE AND TrueValue = TRUE) AS up_correct,
            COUNT(*) FILTER (WHERE PredictedValue = FALSE AND TrueValue = FALSE) AS down_correct,
            COUNT(*) FILTER (WHERE PredictedValue = TrueValue) AS total_correct,
            CASE WHEN COUNT(*) FILTER (WHERE PredictedValue = TRUE) = 0 THEN NULL ELSE 
                COUNT(*) FILTER (WHERE PredictedValue = TRUE AND TrueValue = TRUE) * 100.0 / COUNT(*) FILTER (WHERE PredictedValue = TRUE) END AS up_accuracy,
            CASE WHEN COUNT(*) FILTER (WHERE PredictedValue = FALSE) = 0 THEN NULL ELSE 
                COUNT(*) FILTER (WHERE PredictedValue = FALSE AND TrueValue = FALSE) * 100.0 / COUNT(*) FILTER (WHERE PredictedValue = FALSE) END AS down_accuracy,
            CASE WHEN COUNT(*) = 0 THEN NULL ELSE 
                COUNT(*) FILTER (WHERE PredictedValue = TrueValue) * 100.0 / COUNT(*) END AS total_accuracy
        FROM Predictoor
        WHERE CAST(STRPTIME(Timestamp, '%m/%d/%Y %H:%M') AS DATE) BETWEEN '{formatted_start_date}' AND '{formatted_end_date}'
        GROUP BY TradingPair;
        """
        df = con.execute(query).fetchdf()
        st.write(df)

        # Visualization for total_profit table
        numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
        if len(numeric_columns) > 0:
            st.bar_chart(df.set_index(df.columns[0])[numeric_columns])

    elif selected_table == 'total_profit':
        # Query for the total_profit table with a total row
        query = f"""
        SELECT
        tp.TradingPair,
        tp.total_stake,
        tp.total_payout,
        tp.net_profit,
        ap.total_accuracy
    FROM
        (SELECT
            TradingPair,
            SUM(Stake) AS total_stake,
            SUM(Payout) AS total_payout,
            SUM(Payout) - SUM(Stake) AS net_profit
        FROM Predictoor
        WHERE CAST(STRPTIME(Timestamp, '%m/%d/%Y %H:%M') AS DATE) BETWEEN '{formatted_start_date}' AND '{formatted_end_date}'
        GROUP BY TradingPair) AS tp
    LEFT JOIN
        (SELECT
            TradingPair,
            CASE WHEN COUNT(*) = 0 THEN NULL ELSE 
                COUNT(*) FILTER (WHERE PredictedValue = TrueValue) * 100.0 / COUNT(*) END AS total_accuracy
        FROM Predictoor
        WHERE CAST(STRPTIME(Timestamp, '%m/%d/%Y %H:%M') AS DATE) BETWEEN '{formatted_start_date}' AND '{formatted_end_date}'
        GROUP BY TradingPair) AS ap
    ON tp.TradingPair = ap.TradingPair

    UNION ALL

    SELECT
        'TOTAL' AS TradingPair,
        SUM(Stake) AS total_stake,
        SUM(Payout) AS total_payout,
        SUM(Payout) - SUM(Stake) AS net_profit,
        NULL AS total_accuracy
    FROM Predictoor
    WHERE CAST(STRPTIME(Timestamp, '%m/%d/%Y %H:%M') AS DATE) BETWEEN '{formatted_start_date}' AND '{formatted_end_date}'
    """
        df = con.execute(query).fetchdf()
        st.write(df)

        # Visualization for total_profit table
        numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
        if len(numeric_columns) > 0:
            st.bar_chart(df.set_index(df.columns[0])[numeric_columns])

    else:
        # Default query for other tables
        query = f"SELECT * FROM {selected_table}"
        df = con.execute(query).fetchdf()
        st.write(df)

        # Visualization for total_profit table
        numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
        if len(numeric_columns) > 0:
            st.bar_chart(df.set_index(df.columns[0])[numeric_columns])

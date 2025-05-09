import csv
import os
import time
from io import StringIO
import logging
import requests
import pymysql
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
load_dotenv(override=True)

CSV_FILENAME = 'test_warehouse.csv'
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_ANALYTICS_ORGID = os.getenv("ZOHO_ANALYTICS_ORGID")
ZOHO_WORKSPACE_ID = os.getenv('ZOHO_WORKSPACE_ID_1')
ZOHO_VIEW_ID = os.getenv("ZOHO_VIEW_ID_ZOHOANALYTICS")
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT"))
}

# Function to fetch the access token (Retries until successful)


def fetch_access_token():
    while True:
        try:
            logging.info(
                "[Zoho Analytics: 1] Fetching Access Token from Zoho Account")
            params = {
                'client_id': ZOHO_CLIENT_ID,
                'client_secret': ZOHO_CLIENT_SECRET,
                'grant_type': 'refresh_token',
                'refresh_token': ZOHO_REFRESH_TOKEN
            }
            response = requests.post(
                'https://accounts.zoho.com/oauth/v2/token', params=params)

            if response.status_code == 200:
                access_token = response.json().get('access_token')
                logging.info(
                    f"[Zoho Analytics: 1] Access token fetched successfully")
                return access_token
            else:
                logging.error(
                    f"[Zoho Analytics: 1] Failed to fetch access token. Retrying in 1h...")
                time.sleep(3600)
        except Exception as e:
            logging.error(f"[Zoho Analytics: 1] Error fetching access token: {
                          str(e)}. Retrying in 1h...")
            time.sleep(3600)

# Function to fetch CSV data (Retries until successful)


def fetch_csv_database(access_token):
    while True:
        try:
            logging.info("[Zoho Analytics: 2] Fetching Zoho Analytics Data")

            # Hit the first API for creating data based on query

            headers = {
                'Authorization': f'Bearer {access_token}',
                'ZANALYTICS-ORGID': ZOHO_ANALYTICS_ORGID
            }

            url = 'https://analyticsapi.zoho.com/restapi/v2/bulk/workspaces/' + \
                ZOHO_WORKSPACE_ID + '/views/' + ZOHO_VIEW_ID + \
                "/data?CONFIG={'responseFormat':'csv'}"
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                job_id = response.json()['data']['jobId']

                # Hit the second API for extracting CSV

                headers = {
                    'Authorization': f'Bearer {access_token}',
                    'ZANALYTICS-ORGID': ZOHO_ANALYTICS_ORGID
                }
                url = f"https://analyticsapi.zoho.com/restapi/v2/bulk/workspaces/" + \
                    ZOHO_WORKSPACE_ID + "/exportjobs/" + job_id+"/data"

                response_csv = requests.get(url, headers=headers)

                with open(CSV_FILENAME, 'wb') as file:
                    file.write(response_csv.content)

                logging.info(
                    "[Zoho Analytics: 2] Successfully fetched and saved Zoho Analytics Data")

                return response_csv.content

            else:
                logging.error(
                    f"[Zoho Analytics: 2] Failed to fetch data. Retrying in 1h...")
                time.sleep(3600)
        except Exception as e:
            logging.error(f"[Zoho Analytics: 2] Error fetching data: {
                          str(e)}. Retrying in 1h...")
            time.sleep(3600)

# Function to parse CSV (Retries until successful)


def parse_csv_response(response_data):
    while True:
        try:
            logging.info("[Zoho Analytics: 3] Parsing CSV data")
            csv_data = StringIO(response_data.decode('utf-8'))
            csv_reader = csv.reader(csv_data)
            headers = next(csv_reader)
            rows = [row for i, row in enumerate(csv_reader)]
            logging.info(
                "[Zoho Analytics: 3] CSV parsing completed successfully")
            return headers, rows
        except Exception as e:
            logging.error(f"[Zoho Analytics: 3] Error parsing CSV: {
                          str(e)}. Retrying in 1h...")
            time.sleep(3600)

# Function to maintain database connection


def connect_to_mariadb():
    while True:
        try:
            logging.info("[Zoho Analytics: 4] Connecting to MariaDB")

            # Connect without specifying the database
            connection = pymysql.connect(
                host=DB_CONFIG["host"],
                user=DB_CONFIG["user"],
                password=DB_CONFIG["password"],
                port=DB_CONFIG["port"]
            )
            cursor = connection.cursor()

            # Check if the database exists
            cursor.execute("SHOW DATABASES")
            databases = [db[0] for db in cursor.fetchall()]

            if DB_CONFIG["database"] not in databases:
                cursor.execute(f"CREATE DATABASE {DB_CONFIG['database']}")

            cursor.close()
            connection.close()

            # Now connect again, this time with the correct database
            connection = pymysql.connect(**DB_CONFIG)
            logging.info(
                "[Zoho Analytics: 4] Connected to MariaDB successfully")
            return connection
        except Exception as e:
            logging.error(f"[Zoho Analytics: 4] Failed to connect to MariaDB: {
                          str(e)}. Retrying in 1h...")
            time.sleep(3600)


# Function to upload data (Retries until successful)


def upload_to_mariadb(headers, rows, connection):
    while True:
        try:
            logging.info("[Zoho Analytics: 5] Uploading data to MariaDB")
            cursor = connection.cursor()
            table_name = "zoho_analytics_data"

            # Drop existing table before recreating
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            logging.info(
                f"[Zoho Analytics: 5] Dropped existing table: {table_name}")

            # Escape headers for column names
            escaped_headers = [
                f"`{header.replace(' ', '_')}`" for header in headers]

            # Create table
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {escaped_headers[0]} VARCHAR(255),
                {escaped_headers[1]} VARCHAR(255),
                {escaped_headers[2]} VARCHAR(255),
                {escaped_headers[3]} INT,
                {escaped_headers[4]} VARCHAR(255),
                {escaped_headers[5]} VARCHAR(255)
            )
            """
            cursor.execute(create_table_query)

            # Insert data
            insert_query = f"""
            INSERT INTO {table_name} ({', '.join(escaped_headers)})
            VALUES ({', '.join(['%s' for _ in headers])})
            """
            cleaned_rows = [[None if value == '' else int(
                value) if value.isdigit() else value for value in row] for row in rows]
            cursor.executemany(insert_query, cleaned_rows)
            connection.commit()

            logging.info(f"[Zoho Analytics: 5] Successfully uploaded {
                         len(cleaned_rows)} rows to MariaDB")
            return  # Exit loop after success
        except Exception as e:
            logging.error(f"[Zoho Analytics: 5] Error uploading to MariaDB: {
                          str(e)}. Retrying in 1h...")
            time.sleep(3600)

# Main Process (Runs Until Success)


def zoho_analytics_runner():
    logging.info("[Zoho Analytics: 0] Starting integration process...")

    while True:
        access_token = fetch_access_token()
        response_data = fetch_csv_database(access_token)
        headers, rows = parse_csv_response(response_data)
        if headers and rows:
            break

    while True:
        connection = connect_to_mariadb()
        upload_to_mariadb(headers, rows, connection)
        connection.close()
        logging.info(
            "[Zoho Analytics: 6] Integration process completed successfully!")
        break


# Run the script
if __name__ == '__main__':
    zoho_analytics_runner()

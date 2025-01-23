import csv
import datetime
from io import StringIO
import logging
import requests
import pymysql

"""
Alt3 Integration process

1. Fetching Access Token from Zoho Account
2. Fetching Database as CSV from Zoho Analytics
3. Preprocess Data
4. Maintain Connection to MariaDb
5. CRUD to MariaDb
6. [Add] Setup Scheduler
"""

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Initializing datetime
current_time = ''


def fetch_access_token():
    try:
        logging.info("[1] Fetching Access Token from Zoho Account")

        params = {
            'client_id': '1000.A3E4OXECCIVFV9HV33FULJQO759D1P',
            'client_secret': '3e7cf929839de740f555f25f6276ac7ddf8df85d44',
            'grant_type': 'refresh_token',
            'refresh_token': '1000.63ae04245708415df38c0082c5f1d471.cd3bac74cc738ae31f5b6dcb51689499'
        }

        response = requests.post(
            'https://accounts.zoho.com/oauth/v2/token', params=params)

        if response.status_code == 200:
            data = response.json()
            access_token = data.get('access_token')
            logging.info(f'[1] Access token fetched successfully: {
                         access_token}')
            return access_token
        else:
            logging.error(f"[1] Failed to fetch access token. Status code: {
                          response.status_code}, Response: {response.text}")
            return None
    except:
        logging.error(
            f"[1] An error occurred while fetching the access token: {str(e)}")
        return None


def fetch_csv_database(access_token):
    try:
        logging.info("[2] Fetching Access Token from Zoho Analytics")

        headers = {
            'Authorization': f'Zoho-oauthtoken {access_token}',
            'ZANALYTICS-ORGID': f'761703918'
        }

        response = requests.get(
            'https://analyticsapi.zoho.com/restapi/v2/workspaces/2457185000006351001/views/2457185000013242155/data', headers=headers)

        if response.status_code == 200:
            current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'test_warehouse_{current_time}.csv'

            with open(filename, 'wb') as file:
                file.write(response.content)

            logging.info("[2] Successfully fetched Zoho Analytics Data")

        return response.content

    except:
        logging.error(
            f"[2] An error occurred while fetching the Zoho Analytics: {str(e)}")
        return None


def parse_csv_response(response_data):
    try:
        logging.info("[3] Parsing CSV data from the response")
        csv_data = StringIO(response_data.decode('utf-8'))
        csv_reader = csv.reader(csv_data)
        headers = next(csv_reader)

        # Take only the first 5 rows for testing purpose
        rows = [row for i, row in enumerate(csv_reader) if i < 5]
        logging.info("[3] CSV parsing completed successfully")
        return headers, rows
    except Exception as e:
        logging.error(f"[3] An error occurred while parsing CSV: {str(e)}")
        return None, None

# TODO: Mantain rigid connection


def connect_to_mariadb():
    try:
        logging.info("[4] Connecting to MariaDB")
        connection = pymysql.connect(
            host="34.124.224.50",
            user="root",
            password="123456",
            database="test_warehouse_stock",
            port="3306"
        )
        logging.info("[4] Connected to MariaDB successfully")
        return connection
    except Exception as e:
        logging.error(f"[4] Failed to connect to MariaDB: {str(e)}")
        return None


def main():
    logging.info('Starting integration process...')

    # Step 1: Fetching Access Token from Zoho Account
    access_token = fetch_access_token()
    if not access_token:
        logging.error('Access token retrieval failed. Exiting process.')
        return

    # Step 2: Fetching Database as CSV from Zoho Analytics
    response_data = fetch_csv_database(access_token)

    # Step 3: Preprocess Data
    headers, rows = parse_csv_response(response_data)

    # Step 4: Maintain Connection to MariaDb
    connection = connect_to_mariadb()


if __name__ == '__main__':
    main()

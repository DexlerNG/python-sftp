import configparser
import requests
import psycopg2 as pg
import pandas as pd
import zipfile
import os
import time

# Gets the full path to the directory this "main.py" file is contained in
# Because it is reading from a virtual environment
path = os.path.dirname(os.path.realpath(__file__))
os.chdir(path)

# Initialize the config parser
config = configparser.ConfigParser()

# Read the config file
config.read('config.ini')

# Access the config values for PostgresSQL
dbHostName = config['credentials']['dbHostName']
dbName = config['credentials']['dbName']
dbUsername = config['credentials']['dbUsername']
dbPassword = config['credentials']['dbPassword']
dbPort = int(config['credentials']['dbPort'])

def get_connection_2_database(conn=None):
    try:
        with pg.connect(
            host = dbHostName,
            dbname = dbName,
            user = dbUsername,
            password = dbPassword,
            port = dbPort
        ) as conn:
            print('Database connection has been established')

    except Exception as e:
        print(f'Exception occurred while trying to connect to the DB: {e}')
    
    return conn

def generate_access_token():
    client_id = '1000.16WNMIYYASQQB6TFKX6JMHSWPGAWNK'
    client_secret = '41ea2d37a3ffde8cd251aa9d1ff88dc0d3a274e454'
    #refresh_token = '1000.8bc7ac07b0ed28bcb415a116f61d7fce.170cda58dad1affd2bb3b58eb28ffeb5'
    refresh_token = '1000.d083c8bfcea96abfd1ec0006505ae729.d1c9f35d311a5a831a79d9d89a22b5b1'
    
    token_url = "https://accounts.zoho.com/oauth/v2/token"
    
    params = {
	    'refresh_token': refresh_token,
	    'client_id': client_id,
	    'client_secret': client_secret,
	    'grant_type': 'refresh_token'
	}
    
    response = requests.post(token_url, params=params)
    access_token = response.json().get('access_token')
    
    print("Access Token:", access_token)
    return access_token

def split_csv_by_rows(input_csv, rows_per_file=25000, file_name=''):
    # Read the large CSV file in chunks
    chunk_size = rows_per_file
    file_number = 1
    upload_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'upload'))

    for chunk in pd.read_csv(input_csv, chunksize=chunk_size):
        #chunk.to_csv(f'./upload/{file_name}_{file_number}.csv', index=False)
        chunk.to_csv(f'{os.path.join(upload_path, f'{file_name}_{str(file_number)}.csv')}', index=False)

        # Step 2: Create a ZIP file and add the CSV file to it                                   
        with zipfile.ZipFile(f'{os.path.join(upload_path, f'{file_name}_{str(file_number)}.zip')}', 'w') as zipf:
            zipf.write(f'{os.path.join(upload_path, f'{file_name}_{str(file_number)}.csv')}', os.path.basename(f'{os.path.join(upload_path, f'{file_name}_{str(file_number)}.csv')}'))
        
        os.remove(f'{os.path.join(upload_path, f'{file_name}_{str(file_number)}.csv')}')

        file_number += 1

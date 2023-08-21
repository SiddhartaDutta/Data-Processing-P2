import os
from dotenv import load_dotenv

import csv
from google.cloud import storage
import snowflake.connector as snowflake

load_dotenv()

USERNAME = os.getenv('SFUSER')
PASSWORD = os.getenv('SFPWD')
ACCOUNT = os.getenv('SFACC')
WAREHOUSE = os.getenv('SFWH')
DATABASE = os.getenv('SFDB')
SCHEMA = os.getenv('SFSCH')
TABLE = os.getenv('SFTBL')

conn = snowflake.connect(
    user=USERNAME,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
)



with open('test.csv', 'w') as f:


    cur = conn.cursor()

    # Generate .csv
    cur.execute('select * from {}'.format(TABLE))
    record = cur.fetchall()
    #print(record)

    writer = csv.writer(f, delimiter=',')
    writer.writerows(record)

    cur.close()
# exit()

# Upload to GCP bucket

# Upload to BigQuery

cur.close()

storage_client = storage.Client.from_service_account_json('data_de-poc-335220-0c4b2d7dd908.json')
def upload_to_bucket(blob_name, file_path, bucket_name):

    '''

    Upload file to a bucket

    : blob_name  (str) - object name

    : file_path (str)

    : bucket_name (str)

    '''

    bucket = storage_client.get_bucket(bucket_name)

    blob = bucket.blob(blob_name)

    print(blob)

    blob.upload_from_filename(file_path)

    return blob

upload_to_bucket('test-data/test.csv','test.csv','de-dataproc-west1')

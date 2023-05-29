import configparser
import teradatasql
import boto3
import csv
import os

# Load the properties file and read from the properties file
config = configparser.ConfigParser()
config.read('database_Financial_DB.properties')      
host = config.get('database', 'host')
database = config.get('database', 'database')
user = config.get('database', 'user')
password = config.get('database', 'password')
access_key = config.get('database', 'access_key')
secret_key = config.get('database', 'secret_key')
bucket_name = config.get('database', 'bucket_name')
LOCAL_FILE_PATH_CSV = config.get('database', 'LOCAL_FILE_PATH_CSV')
OBJECT_NAME_CSV = config.get('database', 'OBJECT_NAME_CSV')

# Database connections
conn = teradatasql.connect(
    host=host,
    database=database, 
    user=user,
    password=password
)

# Create a cursor
cursor = conn.cursor()

# Execute a query to get table names from teradata database
query = "SELECT DatabaseName, TableName, CreateTimeStamp, LastAlterTimeStamp FROM dbc.Tables WHERE TableKind = 'T' and DatabaseName = 'Financial_DB' ORDER BY TableName;"
cursor.execute(query)

# Get the result as table name
table_names = [row[1] for row in cursor.fetchall()]

# Create connection for s3 bucket  
s3 = boto3.client('s3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key)

# To fetch the data from teradata database tables and uploading to s3 bucket in csv format
for table in table_names:
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM {}'.format(table))
    results = cursor.fetchall()
    cursor.close()

    
    # Define the filename for the CSV file in upper-case and removing extra spaces
    filename = table.upper().replace(" ", "") + ".csv"

    # Write the data to the CSV file 
    with open(filename, "w", newline="", encoding='utf-8') as csvfile: 
        writer = csv.writer(csvfile) 
        writer.writerows(results)

    # Define the filename for the CSV file to be uploaded to S3
    s3_filename = table.upper().replace(" ", "") + ".csv"

    # Upload the file to S3 bucket 
    s3.upload_file(Filename=filename, Bucket=bucket_name , Key=f'{OBJECT_NAME_CSV}/{s3_filename}')

print("Files Uploaded successfully.")

# Close the connection
conn.close()

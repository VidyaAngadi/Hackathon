import configparser
import teradatasql
import boto3

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
LOCAL_FILE_PATH_DDL= config.get('database', 'LOCAL_FILE_PATH_DDL')
OBJECT_NAME_DDL = config.get('database', 'OBJECT_NAME_DDL')

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

# Create an output file for SQL DDL's
output_file = open(f'{database}.sql', 'w')

# Iterate over table names and extract table DDLs
for table_name in table_names:
    cursor = conn.cursor()
    cursor.execute('SHOW TABLE {}.{}'.format(database, table_name))
    results = cursor.fetchall()
    cursor.close()

    # Write the DDL to the output file
    ddl = results[0][0]
    output_file.write('-- DDL for table {}\n'.format(table_name))
    output_file.write(ddl)
    output_file.write('\n\n')

# Close the output file
output_file.close()

# Create connection for s3 bucket 
s3 = boto3.client('s3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key)

# Upload the file to S3 bucket
s3.upload_file(LOCAL_FILE_PATH_DDL, bucket_name, OBJECT_NAME_DDL)
print("File uploaded successfully.")

# Close the connection
conn.close()

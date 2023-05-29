import re
import boto3
import configparser

# Load the properties file
config = configparser.ConfigParser()
config.read('database.properties')

# Get the properties for the database connection
sourceFile = config.get('database', 'sourceFile') # path to the input file
TEXT_FILE_DDL_CONVERSION = config.get('database', 'TEXT_FILE_DDL_CONVERSION') # path to the DDL conversion file
TABLE_NAME=config.get('database', 'TABLE_NAME') # name of the database table
text_file = open(TEXT_FILE_DDL_CONVERSION, "r")
file_contents = text_file.read()
text_file.close()

def convert_to_sf_ddl(ddl):
    """Converts DDL statements from Teradata to Snowflake syntax."""
    temp_ddl = ddl.upper()\
        .replace("INTEGER", "NUMBER")\
        .replace("SMALLINT", "NUMBER")\
        .replace("BIGINT", "NUMBER")\
        .replace("BYTEINT", "NUMBER")\
        .replace("DECIMAL", "NUMBER")\
        .replace("BINARYINT", "NUMBER")\
        .replace("BYTE", "BINARY")\
        .replace(f'{TABLE_NAME}.', f'{TABLE_NAME}.PUBLIC.')
    
    snowflake_ddl = temp_ddl.replace("NO BEFORE JOURNAL,", "") \
        .replace("NO AFTER JOURNAL,", "") \
        .replace("CHECKSUM = DEFAULT,", "") \
        .replace("DEFAULT MERGEBLOCKRATIO,", "") \
        .replace("MAP = TD_MAP1", "") \
        .replace("CHARACTER SET LATIN NOT CASESPECIFIC", "") \
        .replace("CHARACTER SET LATIN UPPERCASE NOT CASESPECIFIC", "") \
        .replace("CHARACTER SET UNICODE NOT CASESPECIFIC", "") \
        .replace("NOT NULL)", "NOT NULL,") \
        .replace("UNIQUE PRIMARY INDEX", "PRIMARY KEY") \
        .replace("PRIMARY INDEX ", "PRIMARY KEY") \
        .replace("))", "),") \
        .replace(" ))", "))") \
        .replace(" ,", ",") \
        .replace("FORMAT 'YYYY-MM-DD'", "") \
        .replace("  ", " ") \
        .replace("REFERENCES WITH NO CHECK OPTION", "REFERENCES") \
        .replace(" IN ", "") \
        .replace("CREATE SET TABLE", "CREATE OR REPLACE TABLE") \
        .replace(",FALLBACK,", "") \
        .replace(");", "));") \
        .replace("FOREIGN KEY", "-- FOREIGN KEY")
    
    sf_ddl = re.sub(r"\s+\(", "(", snowflake_ddl)
    return sf_ddl

# Convert the input file to Snowflake-compatible DDL
converted_ddl = convert_to_sf_ddl(file_contents)

# Write the converted DDL to the output file
with open(sourceFile, 'w') as f:
    f.write(converted_ddl)

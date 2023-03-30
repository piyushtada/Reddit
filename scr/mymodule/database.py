import psycopg2
from configparser import ConfigParser
import pandas as pd

def get_db_connection(confi_file):
    # Read the configuration file
    config = ConfigParser()
    config.read(confi_file)

    # Get the PostgreSQL connection information
    db_info = config['postgresql']
    host = db_info['host']
    port = db_info['port']
    database = db_info['database']
    username = db_info['username']
    password = db_info['password']

    # Connect to the database and return the connection object
    conn = psycopg2.connect(host=host, port=port, dbname=database, user=username, password=password)
    return conn

def sql_to_dataframe(conn, query, column_names):
   """
   Import data from a PostgreSQL database using a SELECT query 
   """
   
   cursor = conn.cursor()
   try:
      cursor.execute(query)
   except (Exception, psycopg2.DatabaseError) as error:
      print("Error: %s" % error)
      cursor.close()
      return 1
   # The execute returns a list of tuples:
   tuples_list = cursor.fetchall()
   cursor.close()
   # Now we need to transform the list into a pandas DataFrame:
   df = pd.DataFrame(tuples_list, columns=column_names)
   return df

def sql_to_pandas(query):
    # Establish a connection to the PostgreSQL database
    conn = get_db_connection("scr/config.ini")
    # Execute the query
    df = pd.read_sql(query, conn)

    # Close the database connection
    conn.close()

    # Return the resulting DataFrame
    return df
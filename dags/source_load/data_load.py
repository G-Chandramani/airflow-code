import pandas as pd

# Snowflake connector libraries
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas
import boto3

ssm = boto3.client('ssm',region_name='us-east-1')
s3 = boto3.client('s3',region_name='us-east-1')

## Please ensure these parameters are added in the AWS Systems Manager and the EC2 instance IAM has access to s3 and the ssm services
sf_username = ssm.get_parameter(Name='/snowflake/usernm', WithDecryption=True)['Parameter']['Value']
sf_password = ssm.get_parameter(Name='/snowflake/pass', WithDecryption=True)['Parameter']['Value']
sf_account = ssm.get_parameter(Name='/snowflake/account', WithDecryption=True)['Parameter']['Value']

##If you have "https://my-dummy-organisation.snowflakecomputing.com/console/login" as your snowflake login url then the account_name is my-dummy-organisation

def run_script():

   #Module to create the snowflake connection and return the connection objects
   def create_connection():
      conn = snow.connect(user=sf_username,
      password=sf_password,
      account=sf_account,
      warehouse="COMPUTE_WH",
      database="PROD",
      schema="DBT_RAW")
      cursor = conn.cursor()
      print('SQL Connection Created')
      return cursor,conn

   # Module to truncate the table if exists. This will ensure duplicate load doesn't happen
   def truncate_table():
      cur,conn=create_connection()
      sql_titles = "TRUNCATE TABLE IF EXISTS TITLES_RAW"
      sql_credits = "TRUNCATE TABLE IF EXISTS CREDITS_RAW"
      cur.execute(sql_titles)
      cur.execute(sql_credits)
      print('Tables truncated')

   #Module to read csv file and load data in Snowflake. Table is created dynamically
   def load_data():
      titles_file = s3.get_object(Bucket='netflixdataanalytics', Key='raw_files/titles.csv')
      credits_file = s3.get_object(Bucket='netflixdataanalytics', Key='raw_files/credits.csv')
      
      cur,conn=create_connection()
      #titles_file = r"C:\Users\gajbh\Downloads\ETL Pipeline with DBT,AIRFLOW,AWS\Code\Code\dbt-code\datasets\titles.csv" # <- Replace with your path.
      delimiter = "," # Replace if you're using a different delimiter.
      #credits_file=r"C:\Users\gajbh\Downloads\ETL Pipeline with DBT,AIRFLOW,AWS\Code\Code\dbt-code\datasets\credits.csv"

      titles_df = pd.read_csv(titles_file['Body'], sep = delimiter)
      print("Titles file read")
      credits_df = pd.read_csv(credits_file['Body'], sep = delimiter)
      print("Credits file read")

      write_pandas(conn, titles_df, "TITLES",auto_create_table=True)
      print('Titles file loaded')
      write_pandas(conn, credits_df, "CREDITS",auto_create_table=True)
      print('Credits file loaded')

      cur = conn.cursor()


      # Close your cursor and your connection.
      cur.close()
      conn.close()

   print("Starting Script")
   truncate_table()
   load_data()


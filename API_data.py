import json
import requests
import pandas as pd
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
import os
from dotenv import load_dotenv

url = 'https://newsapi.org/v2/everything?'  # url of the api

# def data_extract(file):
#     try: 
#         data = requests.get(url, params = json.loads(open(file, 'r').read())).json()
#         return data
#     except PathError:
#         print('Check your input path')

# load_dotenv()

# def data_extract(file):
def data_extract(file):
    try:
        try:
            file_open = open(file, 'r').read()   # file here is basically the path of your file containing the configurations of the url
            file_open = json.loads(file_open)
            print('File loaded successfully!\n')
        except:
            print('Error in your configuration file')
        
        file_open['apiKey'] = os.getenv('apiKey')
        data = requests.get(url, params = file_open).json()
        print('Data extracted successfully!\n')
        return data
    except:
        print('Check your configuration path')
        
extracted_data = data_extract(r'C:\Users\JayashreeArunan\Desktop\Python\Data_extraction\configuration\api_config.json')

df = pd.DataFrame(extracted_data['articles'])
# df.head(5)
print('Data stored in dataframe successfully\n')

df.insert(0, 'source_id', pd.json_normalize(df['source']).get('id'))
df.insert(1, 'source_name', pd.json_normalize(df['source']).get('name'))
df = df.drop('source', axis=1)
df.to_csv('Data.csv')
print('Data has been stored to a csv file.\n')

# Using json file method and environment variable method
def snowflake_credentials(sf_config):
    try:
        sf_file_open = open(sf_config, 'r').read()
        sf_file_open = json.loads(sf_file_open)
        print('Credentials have been obtained')
    except:
        print('Error in file path')

    sf_connect = connect(
        # user = sf_file_open['user'],
        # password = sf_file_open['password'],
        
        user = os.environ.get('sf_user'),
        password = os.environ.get('sf_password'),
        account = sf_file_open['account'],
        role = sf_file_open['role'],
        warehouse = sf_file_open['warehouse'],
        database = sf_file_open['database'],
        schema = sf_file_open['schema']
    )
    
    # sf_connect = connect(
    #     user = sf_file_open.get('user'),
    #     password = sf_file_open.get('password'),
    #     account = sf_file_open.get('account'),
    #     role = sf_file_open.get('role'),
    #     warehouse = sf_file_open.get('warehouse'),
    #     database = sf_file_open.get('database'),
    #     schema = sf_file_open.get('schema')
    # )
    return sf_connect

# using all the credentials from a json file
# write_pandas(snowflake_credentials(r'C:\Users\JayashreeArunan\Desktop\Python\Data_extraction\configuration\sf_config.json'), df, 'NEW_TABLE', auto_create_table=True, overwrite=True)

# using the username and password stored in the system environment varibles and rest of the credentials from the json file
# write_pandas(snowflake_credentials(r'C:\Users\JayashreeArunan\Desktop\Python\Data_extraction\configuration\sf_config.json'), df, 'NEW_TABLE1', auto_create_table=True, overwrite=True)


# Using .env method
load_dotenv()

def snowflake_credentials():
    try:
        sf_connect = connect(
            user = os.getenv('user'),
            password = os.getenv('password'),
            account = os.getenv('account'),
            role = os.getenv('role'),
            warehouse = os.getenv('warehouse'),
            database = os.getenv('database'),
            schema = os.getenv('schema')
        )
        print('Connection successful!\n')
    except:
        print('Error in credentials')
    return sf_connect

try:
    write_pandas(snowflake_credentials(), df, 'NEW_TRY', auto_create_table=True, overwrite=True)
    print('Data loaded into Snowflake environment!')
except:
    print('Error encountered, check parameters again')
import requests
import json
import os
import snowflake.connector
import time
import datetime
import numpy as np
import pandas as pd
import sqlalchemy as sql
from hyperplane import notebook_common as nc
from pycoingecko import CoinGeckoAPI
from snowflake.connector.pandas_tools import pd_writer
from datetime import datetime
from datetime import timedelta


def snowflake_model_permission_grant():
    ctx = snowflake.connector.connect(
        user = snowflake_creds['user'],
        password = snowflake_creds['password'],
        account = snowflake_creds['account'],
        warehouse = snowflake_creds['warehouse'],
        database = snowflake_creds['database']
        )
    cs = ctx.cursor()

    try:
        # Use successful create to allow role grants if in production
        grant_sql = "GRANT SELECT ON TABLE {}.{}.{} TO ROLE METABASE_READ_ONLY;".format(target_db, target_schema, model_name)
        print(grant_sql)
        cs.execute(grant_sql)
        print(cs.fetchone()[0])

    finally:
        cs.close()

    ctx.close()


'''
Function which leverages an api url and specified column logic to ingest response into Snowflake
Args:
api_response = Requests API response from the API url
table_cols = Snowflake SQL for column formatting
'''
def url_snowflake_ingestor(api_response, table_cols):
    ctx = snowflake.connector.connect(
        user = snowflake_creds['user'],
        password = snowflake_creds['password'],
        account = snowflake_creds['account'],
        warehouse = snowflake_creds['warehouse'],
        database = snowflake_creds['database']
        )
    cs = ctx.cursor()
    
    try:
        json_parse_select = "SELECT {columns} FROM TABLE(flatten(input => PARSE_JSON($${json_input}$$)))".format(columns=table_cols, json_input=json.dumps(api_response.json()))
        dml_statement = "create or replace table {target_database}.{target_schema}.{target_model} as ({json_select});".format(target_database=target_db, target_schema=target_schema, target_model=model_name, json_select=json_parse_select)
        
        print(dml_statement)
        
        cs.execute(dml_statement)
        one_row = cs.fetchone()
        print(one_row[0])
        
        snowflake_model_permission_grant()
        
    finally:
        cs.close()

    ctx.close()
    

def api_library_snowflake_ingestor(cg_asset_id='kin'):
    # Set up Snowflake connection for pd to_sql
    conn_string = f"snowflake://{snowflake_creds['user']}:{snowflake_creds['password']}@{account}/{snowflake_creds['database']}/KIN_MAIN_WAREHOUSE?warehouse={snowflake_creds['warehouse']}"
    engine = sql.create_engine(conn_string)

    # For now, only coin gecko is supported by this non url route
    cg = CoinGeckoAPI()

    
    # TODO: Make this API call incremental. It won't impact the result, but is more efficient
    markets = cg.get_coin_market_chart_by_id(
        id=cg_asset_id,
        vs_currency='usd',
        days='max',
        interval='daily_close'
    )
    print(markets)

    # Start with an empty dataframe we can join each keys dataframe to keep in one dataframe
    df_join = pd.DataFrame({})

    for key in markets.keys():
        # Read each key list of values into a dataframe and join to common dataframe
        df = pd.DataFrame.from_dict(markets[key])
        df.columns = ['timestamp', key]

        # If join is empty, set to first dataframe, else merge
        if df_join.empty:
            df_join = df
        else:
            df_join = df_join.merge(df, left_on='timestamp', right_on='timestamp')

    # Adding in a date_key instead of timestamp for better handling in datastore
    df_join["date_key"] = (pd.to_datetime(df_join["timestamp"], unit='ms') - timedelta(days=1)).dt.strftime("%Y-%m-%d")

    # Remove current days price. Remove most recent timestamp as it's ongoing data
    df_curated = df_join[df_join.date_key < datetime.now().strftime("%Y-%m-%d")]
    df_curated = df_curated[df_curated.timestamp < df_curated['timestamp'].max()]
    print(df_curated)

    #Finally, write to Snowflake with our curated data
    with engine.connect() as con:
        # Setting columns to upper is required or method won't work correctly
        df_curated.columns = map(lambda x: str(x).upper(), df_join.columns)
        df_curated.to_sql(model_name.lower(), con=con, index=False, if_exists='replace', method=pd_writer)

    snowflake_model_permission_grant()


# Get the env from our job params. Default to dev if none provided
env = os.environ.get('HYPERPLANE_JOB_PARAMETER_ENV') if os.environ.get('HYPERPLANE_JOB_PARAMETER_ENV') else 'dev'

if nc.is_jhub():  ## if run in sessions
    snowflake_creds_path = "/root/secret/snowflake-etl-creds.json"
    api_keys_path = "/root/secret/api-keys.json"
else:  ## if run as pipeline jobs or sevices
    snowflake_creds_path = "/etc/hyperplane/secrets/snowflake-etl-creds.json"
    api_keys_path = "/etc/hyperplane/secrets/api-keys.json"

# Fetch secrets for snowflake to allow for connection
with open(snowflake_creds_path, 'r') as file:
    snowflake_creds = json.load(file)[env]

# Fetch api keys for enhanced data calls
with open(api_keys_path, 'r') as file:
    api_keys = json.load(file)[env]

# Kin price as default since it doesn't change after a day closes out
api = os.environ.get('HYPERPLANE_JOB_PARAMETER_API_DESCRIPTOR') if os.environ.get('HYPERPLANE_JOB_PARAMETER_API_DESCRIPTOR') else 'kin_price'

api_lookup = {
    "kin_app": {
        "type": "url",
        "url": "https://portal.kin.org/api/tools/apps",
        "cols": """value:index as id, trim(value:name, '"') as name, trim(value:status, '"') as status, trim(value:publicKey, '"') as public_wallet, current_date as created_date, null as updated_date"""
    },
    "kin_price": {
        "type": "dataframe"
    },
    "solana_price": {
        "type": "dataframe"
    }
}

account = snowflake_creds['account']
target_db = snowflake_creds['database']
# Since these are just used as dimensional models, write to our warehouse
target_schema = "KIN_MAIN_WAREHOUSE"

for api in api_lookup.keys():
    model_name = "DIM_{}".format(api).upper()

    print(api)
    if api_lookup[api]['type'] == "url":
        # This is a temporary fix. Endpoint won't accept requests with a python User-Agent
        headers = {
            'User-Agent': 'PostmanRuntime/7.29.2',
            'authorization': api_keys[api]
        }
        r = requests.get(api_lookup[api]['url'], headers=headers)

        if r.status_code == 200:
            print("Valid status code")
            url_snowflake_ingestor(r, api_lookup[api]['cols'])
        else:
            print("Invalid response from {api_name} API. Response code: {response_code}.").format(api_name=api, response_code=str(r.status_code))

    elif api_lookup[api]['type'] == "dataframe":
        print("In df")
        # TODO: Extend this beyond only CoinGecko API in the future
        api_library_snowflake_ingestor(api.split('_')[0])

    else:
        print("Unknown external endpoint type.")

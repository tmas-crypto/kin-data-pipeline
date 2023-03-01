import snowflake.connector
import json
import glob
import os
import time
import numpy as np
import pandas as pd
import sqlalchemy as sql
from hyperplane import notebook_common as nc
from snowflake.connector.pandas_tools import pd_writer
from sqlalchemy import create_engine
from sqlalchemy import JSON, INT, FLOAT, DATE
from slack_sdk.webhook import WebhookClient


# This dict is necessary to override pandas implicit casting. Needed for API db
# TODO: Do this using stored json files in the project. Low priority 2022-12-01
type_override_dict = {
    'dailyAccountSummaryApp': {
        'index': INT,
        'accountsCreated': INT,
        'solCost': FLOAT
    },
    'dailyAccountSummaryEcosystem': {
        'index': INT,
        'accountsCreated': INT,
        'solCost': FLOAT
    },
    'dailyMarketSummary': {
        'date': DATE,
    },
    'kreSummary': {
        'activeApps': INT,
        'activeUsers': INT,
        'dailyTransactions': INT,
        'activeUserBalance': FLOAT,
        'activeCappedUserBalance': FLOAT
    },
    'krePayoutSummary': {
        'top10': JSON
    },
    'dailySummaryApp': {
        'index': INT,
        'transactionFeesUsd': FLOAT
    },
    'dailySummaryEcosystem': {
        'totalDailyAmount': FLOAT,
        'dailyEarnAmount': FLOAT,
        'dailySpendAmount': FLOAT,
        'dailyPeerAmount': FLOAT,
        'monthlyActiveApps': INT,
        'totalDailyTransactions': INT,
        'dailyEarnTransactions': INT,
        'dailySpendTransactions': INT,
        'dailyPeerTransactions': INT,
        'transactionFees': FLOAT,
        'transactionFeesUsd': FLOAT
    }
}

'''
Parameter 0 - Specify pipeline environment
Valid values = dev, qa, prod
'''
env = os.environ.get('HYPERPLANE_JOB_PARAMETER_ENV') if os.environ.get('HYPERPLANE_JOB_PARAMETER_ENV') else 'dev'

'''
Job Parameter 1 - Specify if job run is a full refresh of models or incremental build
Valid values = full_refresh, incremental
'''
build_type = os.environ.get('HYPERPLANE_JOB_PARAMETER_BUILD_FLAG') if os.environ.get('HYPERPLANE_JOB_PARAMETER_BUILD_FLAG') else 'incremental'

'''
Parameter 2 - Specify model path in mart set to be run
Valid values = any directory in the load script path. Ex. daily_models, weekly_models
'''
model_tag = os.environ.get('HYPERPLANE_JOB_PARAMETER_MODEL_TAG') if os.environ.get('HYPERPLANE_JOB_PARAMETER_MODEL_TAG') else 'daily_models'

if nc.is_jhub():
    ## if run in sessions
    snowflake_creds = "/root/secret/snowflake-etl-creds.json"
    postgres_creds = "/root/secret/postgres-creds.json"
else:
    ## if run as pipeline jobs or sevices
    snowflake_creds = "/etc/hyperplane/secrets/snowflake-etl-creds.json"
    postgres_creds = "/etc/hyperplane/secrets/postgres-creds.json"

dir_path = './tyler_scheduling_poc/load_scripts/postgres_replication/{model_tag}/**/*.sql'

pg_conn = 'postgresql://{user_id}:{user_password}@{server}:{port}/{database}'

url = "my_slack_webhook"
webhook = WebhookClient(url)

try:
    # Fetch secrets for snowflake to allow for connection
    with open(snowflake_creds, 'r') as sf_file:
        snowflake_creds = json.load(sf_file)[env]

    # Establish connection to Snowflake
    sf_ctx = snowflake.connector.connect(
        user = snowflake_creds['user'],
        password = snowflake_creds['password'],
        account = "snowflake_creds['account']",
        warehouse = snowflake_creds['warehouse'],
        database = snowflake_creds['database']
        )
    cs = sf_ctx.cursor()
    
    # Fetch secrets for pg conn
    with open(postgres_creds, 'r') as pg_file:
        postgres_creds = json.load(pg_file)[env]

    pg_db = create_engine(pg_conn.format(
        user_id=postgres_creds['user'],
        user_password=postgres_creds['password'],
        server=postgres_creds['server'],
        port=postgres_creds['port'],
        database=postgres_creds['database']
    ))

    pg_ctx = pg_db.connect()
    
    pg_replication_type = 'replace' if build_type == 'full_refresh' else 'append'

    # For now, lets using sorting as a method to control model dependencies
    for script in sorted(glob.glob(dir_path.format(model_tag=model_tag), recursive=True)):
        
        source_db = snowflake_creds['database']
        model_name = script.split('/')[-1].split('.')[0]
        
        date_filter = ""
        if pg_replication_type == 'append':
            with pg_ctx.connect() as incremental_con:

                # Standardize date columns betweeb models
                date_col = "DATE_KEY"
                if model_name in type_override_dict.keys():
                    date_col = "date"

                incremental_key = pd.read_sql_query('SELECT max("{date_field}") as incremental_key FROM "{model}"'.format(date_field=date_col, model=model_name), con=incremental_con)['incremental_key'].values[0]
                print("Incremental key = {incremental_key} for model {model}".format(incremental_key=incremental_key, model=model_name))

            date_filter = "WHERE DATE_KEY > '{pg_max_date}'".format(pg_max_date=str(incremental_key))

        model_start = time.time()
        print("Execution of {model} start: {epoch_time} \n".format(model=model_name, epoch_time=model_start))
        
        with open(script) as file:            
            # Default behaviour
            
            # Read script from file and execute against Snowflake
            file_contents = file.read().format(source_database=source_db, incremental_filter=date_filter)
            print(file_contents)
            
            # Execute select to get data and parse out rows in pandas df
            cs.execute(file_contents)
            data = cs.fetch_pandas_all()
            print(data)

            # Type overrides
            dtypes=None
            if model_name in type_override_dict.keys():
                dtypes = type_override_dict[model_name]

            # Write data to postgres for data api
            data.to_sql(model_name, pg_ctx, index=False, if_exists = pg_replication_type, dtype = dtypes)

            # Require this index to be set to PK for Prisma introspection. Column is automatically added. Only required when table is rebuilt
            if pg_replication_type == 'replace':
                with pg_ctx.connect() as con:
                    con.execute('ALTER TABLE "{model}" ADD COLUMN "id" SERIAL PRIMARY KEY;'.format(model=model_name))
                
                
            
        model_finish = time.time()
        print("Execution of {model} finished: {epoch_time}".format(model=model_name, epoch_time=model_finish))
        print("Model run time: {time_diff} seconds \n".format(time_diff=model_finish-model_start))

except Exception as e:
    # Display error in console
    print('An exception has occured:')
    print('---------------------------------------------------------------------')
    print(e)
    print('---------------------------------------------------------------------')

    # Send notification of failure to Slack for review
    response = webhook.send(text="Postgres Replication Failed.")
    assert response.status_code == 200
    assert response.body == "ok"

else:
    # Send notification of success to Slack
    response = webhook.send(text="Postgres Replication Successful.")
    assert response.status_code == 200
    assert response.body == "ok"

finally:
    cs.close()
    sf_ctx.close()

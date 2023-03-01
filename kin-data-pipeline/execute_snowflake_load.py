import snowflake.connector
import json
import glob
import os
import sys
import time
from hyperplane import notebook_common as nc
from slack_sdk.webhook import WebhookClient

'''
Job Parameter 0 - Specify pipeline environment
Valid values = dev, qa, prod
'''
env = os.environ.get('HYPERPLANE_JOB_PARAMETER_ENV') if os.environ.get('HYPERPLANE_JOB_PARAMETER_ENV') else 'dev'

'''
Job Parameter 1 - Specify if job run is a full refresh of models or incremental build
Valid values = full_refresh, incremental
'''
build_type = os.environ.get('HYPERPLANE_JOB_PARAMETER_BUILD_FLAG') if os.environ.get('HYPERPLANE_JOB_PARAMETER_BUILD_FLAG') else 'incremental'

'''
Job Parameter 2 - Specify model path in mart set to be run
Valid values = any directory in the load script path. Ex. daily_models, weekly_models
'''
model_tag = os.environ.get('HYPERPLANE_JOB_PARAMETER_MODEL_TAG') if os.environ.get('HYPERPLANE_JOB_PARAMETER_MODEL_TAG') else 'daily_models'

'''
YAML Input 1 - Specify data mart models to be run
Valid values = any existing mart in Snowflake. Ex: METABASE_MART, KRE_MART
Default = None
'''
# Using a list to mainain schema order or operations for our models
mart_list = ['kin_main_warehouse', 'kre_mart', 'metabase_mart', 'data_studio_mart', 'kin_data']

if nc.is_jhub():
    ## if run in sessions
    snowflake_creds = "/root/secret/snowflake-etl-creds.json"
else:
    ## if run as pipeline jobs or sevices
    snowflake_creds = "/etc/hyperplane/secrets/snowflake-etl-creds.json"

dir_path = './tyler_scheduling_poc/load_scripts/{data_mart}/{model_tag}/**/*.sql'

incremental_epoch_dict = {
    'daily_models': 'day',
    'weekly_models': 'week',
    'monthly_models': 'month'
}

url = "my_slack_webhook"
webhook = WebhookClient(url)

try:
    # Fetch secrets for snowflake to allow for connection
    with open(snowflake_creds, 'r') as file:
        snowflake_creds = json.load(file)[env]

    # Establish connection to Snowflake
    ctx = snowflake.connector.connect(
        user = snowflake_creds['user'],
        password = snowflake_creds['password'],
        account = snowflake_creds['account'],
        warehouse = snowflake_creds['warehouse'],
        database = snowflake_creds['database']
        )
    cs = ctx.cursor()
    
    for mart_tag in mart_list:
        # This sort manages our model dependencies at the schema level
        for script in sorted(glob.glob(dir_path.format(data_mart=mart_tag, model_tag=model_tag), recursive=True)):

            target_db = snowflake_creds['database']
            target_schema = mart_tag.upper()
            # This assumes that the last file name is the same as the model name in Snowflake
            model_name = script.split('/')[-1].split('.')[0]

            model_start = time.time()
            print("Execution of {model} start: {epoch_time} \n".format(model=model_name, epoch_time=model_start))

            with open(script) as file:
                # Inject incremental sql into model runs
                with open('./tyler_scheduling_poc/incremental_anchor.sql') as date_anchor:
                    incremental_date = date_anchor.read().format(target_database=target_db, target_schema=target_schema, target_model=model_name)
                    create_or_insert = 'insert into {target_database}.{target_schema}.{target_model} with {incremental_sql}'.format(target_database=target_db, target_schema=target_schema, target_model=model_name, incremental_sql=incremental_date)
                    query_close = ';'

                # Adjust SQL if full refresh of model
                if build_type != 'incremental':
                    create_or_insert = "create or replace table {target_database}.{target_schema}.{target_model} as ( with ".format(target_database=target_db, target_schema=target_schema, target_model=model_name)
                    query_close = ");"

                # Read script from file and execute against Snowflake
                if 'clones' in script:
                    # We want to handle cloning from other marts differently from create/insert
                    file_contents = file.read().format(target_database=target_db, target_schema=target_schema, target_model=model_name)
                else:
                    file_contents = file.read().format(
                        create_or_insert=create_or_insert,
                        incremental_filter="and date_trunc('{date_unit}', date_key::DATE) > (select last_insert_date from incremental_date)".format(date_unit=incremental_epoch_dict[model_tag.split('/')[0]]) if build_type == 'incremental' else "",
                        query_close=query_close,
                        target_database=target_db)
                print(file_contents)

                cs.execute(file_contents)
                one_row = cs.fetchone()
                print(one_row[0])

                # Use successful create/clones to allow role grants
                if build_type == 'full_refresh' or 'clones' in script:
                    if mart_tag == 'data_studio_mart':
                        role = "DATA_STUDIO_READ_ONLY_{}".format(env.upper())
                    elif mart_tag == 'kin_data':
                        role = "KIN_DATA_READ_ONLY".format(env.upper())
                    else:
                        role = "METABASE_READ_ONLY".format(env.upper())

                    grant_sql = "GRANT SELECT ON TABLE {}.{}.{} TO ROLE {};".format(target_db, target_schema, model_name, role)
                    print(grant_sql)
                    cs.execute(grant_sql)
                    print(cs.fetchone()[0])

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
    response = webhook.send(text="Snowflake Model Load Failed.")
    assert response.status_code == 200
    assert response.body == "ok"

else:
    # Send notification of success to Slack
    response = webhook.send(text="Snowflake Model Load Successful.")
    assert response.status_code == 200
    assert response.body == "ok"

finally:
    cs.close()
    ctx.close()

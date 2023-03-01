# Main purpose of this module is to cleanup our models from some date. Ex: RPC failure
import snowflake.connector
import json
import glob
import os
import sys
import time
from hyperplane import notebook_common as nc
from datetime import date

'''
Job Parameter 0 - Specify pipeline environment
Valid values = dev, qa, prod
'''
env = os.environ.get('HYPERPLANE_JOB_PARAMETER_ENV') if os.environ.get('HYPERPLANE_JOB_PARAMETER_ENV') else 'dev'

'''
Job Parameter 1 - Specify model path in mart set to be run
Valid values = any directory in the load script path. Ex. daily_models, weekly_models
'''
model_tag = os.environ.get('HYPERPLANE_JOB_PARAMETER_MODEL_TAG') if os.environ.get('HYPERPLANE_JOB_PARAMETER_MODEL_TAG') else 'daily_models'

'''
Job Parameter 2 - Specify date to delete data from models
Valid values = any date
'''
cleanup_date = os.environ.get('HYPERPLANE_JOB_PARAMETER_CLEANUP_DATE') if os.environ.get('HYPERPLANE_JOB_PARAMETER_CLEANUP_DATE') else date.today()

# Date logic handling for models based on cadence
if 'weekly_models' in model_tag:
    cleanup_date = "date_trunc('week', '{date_key}'::DATE)".format(date_key=cleanup_date)
elif 'monthly_models' in model_tag:
    cleanup_date = "date_trunc('month', '{date_key}'::DATE)".format(date_key=cleanup_date)
else:
    cleanup_date = "'{date_key}'::DATE".format(date_key=cleanup_date)

'''
YAML Input 1 - Specify data mart models to be run
Valid values = any existing mart in Snowflake. Ex: METABASE_MART, KRE_MART
Default = None
'''
mart_list = ['kin_main_warehouse', 'kre_mart', 'metabase_mart', 'data_studio_mart', 'kin_data']

if nc.is_jhub():
    ## if run in sessions
    snowflake_creds = "/root/secret/snowflake-etl-creds.json"
    # env = 'tyler_dev'
else:
    ## if run as pipeline jobs or sevices
    snowflake_creds = "/etc/hyperplane/secrets/snowflake-etl-creds.json"

dir_path = './tyler_scheduling_poc/load_scripts/{data_mart}/{model_tag}/**/*.sql'

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
        for script in sorted(glob.glob(dir_path.format(data_mart=mart_tag, model_tag=model_tag), recursive=True)):

            target_db = snowflake_creds['database']
            target_schema = mart_tag.upper()
            # This assumes that the last file name is the same as the model name in Snowflake
            model_name = script.split('/')[-1].split('.')[0]

            # This is necessary as we never want to clean this model based on date key
            if model_name != 'multi_create_impacted_accounts':
                model_start = time.time()
                print("Execution of {model} start: {epoch_time} \n".format(model=model_name, epoch_time=model_start))
            
                cleanup_statement = "DELETE FROM {target_database}.{target_schema}.{target_model} WHERE DATE_KEY >= {target_date};".format(target_database=target_db, target_schema=target_schema, target_model=model_name, target_date=cleanup_date)
                print(cleanup_statement)


                cs.execute(cleanup_statement)
                one_row = cs.fetchone()
                print(one_row[0])

                model_finish = time.time()
                print("Execution of {model} finished: {epoch_time}".format(model=model_name, epoch_time=model_finish))
                print("Model run time: {time_diff} seconds \n".format(time_diff=model_finish-model_start))
            
finally:
    cs.close()
    ctx.close()

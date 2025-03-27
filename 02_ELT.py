from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task
from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.operators.python import get_current_context

import logging
import snowflake.connector

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def run_ctas(database, schema, table, select_sql, primary_key=None):

    logging.info(table)
    logging.info(select_sql)

    cur = return_snowflake_conn()

    try:
        sql = f"CREATE OR REPLACE TABLE {database}.{schema}.temp_{table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        # do primary key uniquess check
        if primary_key is not None:
            sql = f"""
              SELECT {primary_key}, COUNT(1) AS cnt 
              FROM {database}.{schema}.temp_{table}
              GROUP BY 1
              ORDER BY 2 DESC
              LIMIT 1"""
            print(sql)
            cur.execute(sql)
            result = cur.fetchone()
            print(result, result[1])
            if int(result[1]) > 1:
                print("!!!!!!!!!!!!!!")
                raise Exception(f"Primary key uniqueness failed: {result}")

        # Add another condition for checking duplicates based on userId and timestamp
        duplicate_check_sql = f"""
            SELECT COUNT(*) AS total_rows, 
                   COUNT(DISTINCT userId || '_' || ts) AS distinct_combinations
            FROM {database}.{schema}.temp_{table}
        """
        cur.execute(duplicate_check_sql)
        dup_result = cur.fetchone()
        if dup_result[0] > dup_result[1]:
            duplicate_count = dup_result[0] - dup_result[1]
            print(f"!!!WARNING!!!: Found {duplicate_count} duplicate records based on userId and timestamp")
            # raise Exception(f"Duplicate records detected: {duplicate_count} records have duplicate userId and timestamp combinations")
          
        main_table_creation_if_not_exists_sql = f"""
            CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} AS
            SELECT * FROM {database}.{schema}.temp_{table} WHERE 1=0;"""
        cur.execute(main_table_creation_if_not_exists_sql)

        swap_sql = f"""ALTER TABLE {database}.{schema}.{table} SWAP WITH {database}.{schema}.temp_{table};"""
        cur.execute(swap_sql)
    except Exception as e:
        raise


with DAG(
    dag_id = 'Assign06_02_ELT_Using_CTAS',
    start_date = datetime(2024,10,2),
    catchup=False,
    tags=['ELT'],
    schedule = '45 2 * * *'
) as dag:

    database = "USER_DB_BOA"
    schema = "analytics"
    table = "session_summary"
    select_sql = """SELECT u.*, s.ts
    FROM USER_DB_BOA.raw.user_session_channel u
    JOIN USER_DB_BOA.raw.session_timestamp s ON u.sessionId=s.sessionId
    """

    run_ctas(database, schema, table, select_sql, primary_key='sessionId')
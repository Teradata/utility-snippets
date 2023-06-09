"""
### Vantage DAG Tutorial 
This DAG is demonstrating connecting to Teradata Vantage and executing SQL queries or macros
"""
from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from teradatasql import connect

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def call_vantage_sql(sql, fetchoutput=False, logoutput=False):
    username = Variable.get('vantage_username')
    password = Variable.get('vantage_password')
    database_url = Variable.get('vantage_host')
    logmech =  Variable.get('vantage_logmech')
    
    # Connect to Teradata
    with connect(host=database_url, user=username, password=password, logmech=logmech) as conn:
        cur = conn.cursor()
        
        # Execute the sql
        try:
            cur.execute(sql)
            if fetchoutput or logoutput:
                o=cur.fetchall()
                if logoutput:
                    print(o)
                print(f'Executed SQL statement {sql}, {str(cur.rowcount)} rows affected.')
                return o
        except Exception as e:
            print('Failed executing SQL statement {sql} with error message:')
            print(e.args[0].split('\n')[0])
            raise
        # Close the connection
        cur.close()

def call_vantage_macro(macro_name, input_vars=[], logoutput=False):

    # Parse parameters
    input_vars=','.join([f'{v}' for v in input_vars])
    input_vars=f'({input_vars})' if input_vars else ''

    # Execute macro
    call_vantage_sql(f'EXEC {macro_name}{input_vars}',logoutput)

@dag(dag_id='vantage-dag-simplified', default_args=default_args, schedule_interval=timedelta(days=1), catchup=False)
def example_vantage_workflow():

    # Define methods for tasks
    @task
    def workflowInit():
        call_vantage_sql("sel 'Hello Vantage!';")

    @task
    def createMacro():
        call_vantage_sql("replace macro get_info as (sel InfoData from dbc.dbcinfo where InfoKey='VERSION';);")

    @task
    def vantageMacroVersionInfo():
        call_vantage_macro('get_info', logoutput=True)

    @task
    def vantageTimeCheck():
        call_vantage_sql('sel current_timestamp;')

    @task
    def workflow_close():
        print('Finishing the Workflow!')

    # Create tasks
    workflowInit_t=workflowInit()
    createMacro_t=createMacro()
    vantageMacroVersionInfo_t=vantageMacroVersionInfo()
    vantageTimeCheck_t=vantageTimeCheck()
    workflowClose_t=workflow_close()

    # Define DAG workflow
    workflowInit_t>>createMacro_t>>vantageMacroVersionInfo_t>>workflowClose_t
    workflowInit_t>>vantageTimeCheck_t>>workflowClose_t

dag = example_vantage_workflow()
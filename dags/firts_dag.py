from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

#from extract_data_api import Extract

with DAG('meu_primeiro_dag',
         start_date=days_ago(2),
         schedule_interval='@daily',
         tags=["dev"]
         ) as dag:
    
    tarefa_1 = EmptyOperator(task_id='Task_1')
    tarefa_2 = EmptyOperator(task_id='Task_2')
    tarefa_3 = EmptyOperator(task_id='Task_3')
    tarefa_4 = BashOperator(task_id='create_dir',
                             bash_command= 'mkdir -p $HOME//workspace/data_pipeline_airflow/{{ data_interval_end }}'
                            )
    
    tarefa_1 >> [tarefa_2,tarefa_3] >> tarefa_4



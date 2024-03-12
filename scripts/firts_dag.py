from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

with DAG('meu_primeiro_dag',
         start_date=days_ago(1),
         schedule_interval='@daily',
         tags=["dev"]
         ) as dag:
    
    tarefa_1 = EmptyOperator(task_id='Task_1')
    tarefa_2 = EmptyOperator(task_id='Task_2')
    tarefa_3 = EmptyOperator(task_id='Task_3')
    tarefa_4 = BashOperator(task_id='cria_pasta',
                             bash_command='mkdir -p "/secrets/air2"'
                            )
    
    tarefa_1 >> [tarefa_2,tarefa_3] >> tarefa_4



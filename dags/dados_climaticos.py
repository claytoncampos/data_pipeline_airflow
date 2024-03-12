from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.macros import ds_add
import pendulum
import os
from dotenv import load_dotenv
import pandas as pd
from os.path import join




with DAG('dados_climaticos',
         start_date=pendulum.datetime(2024, 3, 4, tz="UTC"),
         schedule_interval='0 0 * * 1', #executar toda segunda feira
         tags=["development, previsao_tempo"]
         ) as dag:

    init = EmptyOperator(task_id='init')
    

    create_directory = BashOperator(task_id='create_dir',
                                    bash_command= 'mkdir -p $HOME/workspace/data_pipeline_airflow/{{ data_interval_end }}')
    
    def extract_data(data_interval_end):
        load_dotenv()

        API_KEY = os.getenv("KEY")
        
        print(API_KEY)
        print(data_interval_end)
        city = 'Boston'

        URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
                f'{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&key={API_KEY}&include=days&contentType=csv')


        dados = pd.read_csv(URL)

        file_path = f'/home/Clayton/workspace/data_pipeline_airflow/{data_interval_end}/'

        dados.to_csv(file_path + 'dados_brutos.csv')
        dados[['datetime', 'tempmin', 'temp','tempmax']].to_csv(file_path + 'temperatura.csv')
        dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')

    extract = PythonOperator(task_id='extract_data',
                             python_callable=extract_data,
                             op_kwargs= {'data_interval_end':'{{data_interval_end.strftime("%Y-%m-%d")}}'})
    
    end = EmptyOperator(task_id='end')


    init >> create_directory >> extract >> end
    

      
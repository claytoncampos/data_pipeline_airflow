import os
from dotenv import load_dotenv
import pandas as pd
from os.path import join
from datetime import datetime, timedelta

def Extract():
    load_dotenv()

    API_KEY = os.getenv("KEY")


    #intervalo de datas
    data_inicio = datetime.today()
    data_fim = data_inicio + timedelta(days=7)

    # formatando as datas
    data_inicio = data_inicio.strftime('%Y-%m-%d')
    data_fim = data_fim.strftime('%Y-%m-%d')

    city = 'Boston'

    URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
            f'{city}/{data_inicio}/{data_fim}?unitGroup=metric&key={API_KEY}&include=days&contentType=csv')


    dados = pd.read_csv(URL)

    file_path = f'/home/Clayton/workspace/data_pipeline_airflow/semama={data_inicio}/'
    os.mkdir(file_path)

    dados.to_csv(file_path + 'dados_brutos.csv')
    dados[['datetime', 'tempmin', 'temp','tempmax']].to_csv(file_path + 'temperatura.csv')
    dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')
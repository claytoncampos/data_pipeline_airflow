### Projeto de Pipeline da dados

##### Empresa de Turismo precisa obter a previsão de tempo para os próximos 7 dias para conseguir adequadar os melhores passeios e serviços ofertados para seus clientes de acordo com a previsão do tempo.

*   Iremos Utilizar o modelo Medalhão ( Bronze / Silver / Gold)
*   Armazenamento de Previsão do tempo, condições climaticas e temperaturas
*   Orquestração com Apache airflow


1) Para isso faremos a Extração dos dados através de API ( visualcrossing )
2) Iremos armazenar esses dados brutos na camada Bronze
3) Iremos armazenar os dados tratados na camada Silver
4) Iremos armazenar os dados prontos para consumo na camada Gold
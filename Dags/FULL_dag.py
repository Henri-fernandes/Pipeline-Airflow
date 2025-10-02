from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id='FULL_dag',
    start_date=datetime(2025, 9, 29),
    schedule=None,
    catchup=False,
    tags=['full']
) as dag:

    inicio = EmptyOperator(task_id='inicio')

    trigger_credito = TriggerDagRunOperator(
        task_id='disparar_credito',
        trigger_dag_id='GR_ft_credito_liberado_dag'
    )

    trigger_seguro = TriggerDagRunOperator(
        task_id='disparar_seguro',
        trigger_dag_id='GR_ft_seguro_dag'
    )

    trigger_investimento = TriggerDagRunOperator(
        task_id='disparar_investimento',
        trigger_dag_id='GR_ft_investimento_dag'
    )

    # Adicione outros DAGs aqui conforme forem criados
    # Exemplo:
    # trigger_cartao = TriggerDagRunOperator(
    #     task_id='disparar_cartao',
    #     trigger_dag_id='GR_ft_cartao_dag'
    # )

    fim = EmptyOperator(task_id='fim')

    inicio >> [trigger_credito, trigger_seguro, trigger_investimento] >> fim

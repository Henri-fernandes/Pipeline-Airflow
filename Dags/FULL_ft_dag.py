from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id='FULL_ft_pipeline',
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    tags=['orchestrator', 'fatos', 'full_ft', 'otimizado'],
    description='Fatos OTIMIZADAS: crédito + investimento + seguro em paralelo',
    max_active_runs=1,  # OTIMIZAÇÃO: Evita conflitos
) as dag:

    inicio = EmptyOperator(task_id='inicio')

    # TODAS FATOS EM PARALELO (já era otimizado, melhorando apenas o polling)
    # OTIMIZAÇÃO: poke_interval de 5s para detecção 6x mais rápida
    trigger_ft_credito = TriggerDagRunOperator(
        task_id='trigger_GR_ft_credito_liberado',
        trigger_dag_id='GR_ft_credito_liberado_dag',
        wait_for_completion=True,
        poke_interval=5,  # OTIMIZAÇÃO: Verificação 6x mais rápida
        reset_dag_run=True,
        allowed_states=['success'],
        failed_states=['failed'],
    )

    trigger_ft_investimento = TriggerDagRunOperator(
        task_id='trigger_GR_ft_investimento',
        trigger_dag_id='GR_ft_investimento_dag',
        wait_for_completion=True,
        poke_interval=5,  # OTIMIZAÇÃO: Verificação 6x mais rápida
        reset_dag_run=True,
        allowed_states=['success'],
        failed_states=['failed'],
    )

    trigger_ft_seguro = TriggerDagRunOperator(
        task_id='trigger_GR_ft_seguro',
        trigger_dag_id='GR_ft_seguro_dag',
        wait_for_completion=True,
        poke_interval=5,  # OTIMIZAÇÃO: Verificação 6x mais rápida
        reset_dag_run=True,
        allowed_states=['success'],
        failed_states=['failed'],
    )

    fim = EmptyOperator(task_id='fatos_concluidos')

    # FLUXO: Todas fatos em paralelo (máxima eficiência)
    inicio >> [trigger_ft_credito, trigger_ft_investimento, trigger_ft_seguro] >> fim
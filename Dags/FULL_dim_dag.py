from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id='FULL_pipeline',
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    tags=['orchestrator', 'full', 'completo', 'otimizado'],
    description='Pipeline completo OTIMIZADO: dimensões inteligentes + fatos paralelas',
    max_active_runs=1,
) as dag:

    inicio = EmptyOperator(task_id='inicio')

    # ===== DIMENSÃO FILIAL (obrigatória primeiro) =====
    trigger_dim_filial = TriggerDagRunOperator(
        task_id='trigger_GR_dim_filial',
        trigger_dag_id='GR_dim_filial_dag',
        wait_for_completion=True,
        poke_interval=10,  # Aumentado para reduzir carga no scheduler
        reset_dag_run=False,  # ✅ MUDANÇA CRÍTICA: Não resetar execuções
        allowed_states=['success'],
        failed_states=['failed'],
    )

    # ===== DIMENSÕES EM PARALELO =====
    trigger_dim_pessoa = TriggerDagRunOperator(
        task_id='trigger_GR_dim_pessoa',
        trigger_dag_id='GR_dim_pessoa_dag',
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=False,  # ✅ MUDANÇA CRÍTICA
        allowed_states=['success'],
        failed_states=['failed'],
    )

    trigger_dim_produto = TriggerDagRunOperator(
        task_id='trigger_GR_dim_produto',
        trigger_dag_id='GR_dim_produto_dag',
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=False,  # ✅ MUDANÇA CRÍTICA
        allowed_states=['success'],
        failed_states=['failed'],
    )

    checkpoint_pessoa_produto = EmptyOperator(task_id='pessoa_produto_concluidos')

    # ===== DIMENSÃO CONTA =====
    trigger_dim_conta = TriggerDagRunOperator(
        task_id='trigger_GR_dim_conta',
        trigger_dag_id='GR_dim_conta_dag',
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=False,  # ✅ MUDANÇA CRÍTICA
        allowed_states=['success'],
        failed_states=['failed'],
    )

    checkpoint_dims = EmptyOperator(task_id='dimensoes_concluidas')

    # ===== FATOS EM PARALELO =====
    trigger_ft_credito = TriggerDagRunOperator(
        task_id='trigger_GR_ft_credito_liberado',
        trigger_dag_id='GR_ft_credito_liberado_dag',
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=False,  # ✅ MUDANÇA CRÍTICA
        allowed_states=['success'],
        failed_states=['failed'],
    )

    trigger_ft_investimento = TriggerDagRunOperator(
        task_id='trigger_GR_ft_investimento',
        trigger_dag_id='GR_ft_investimento_dag',
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=False,  # ✅ MUDANÇA CRÍTICA
        allowed_states=['success'],
        failed_states=['failed'],
    )

    trigger_ft_seguro = TriggerDagRunOperator(
        task_id='trigger_GR_ft_seguro',
        trigger_dag_id='GR_ft_seguro_dag',
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=False,  # ✅ MUDANÇA CRÍTICA
        allowed_states=['success'],
        failed_states=['failed'],
    )

    fim = EmptyOperator(task_id='pipeline_completo')

    # FLUXO OTIMIZADO
    inicio >> trigger_dim_filial >> [trigger_dim_pessoa, trigger_dim_produto]
    [trigger_dim_pessoa, trigger_dim_produto] >> checkpoint_pessoa_produto >> trigger_dim_conta
    trigger_dim_conta >> checkpoint_dims
    checkpoint_dims >> [trigger_ft_credito, trigger_ft_investimento, trigger_ft_seguro] >> fim

    fim = EmptyOperator(task_id='dimensoes_concluidas')

    # FLUXO OTIMIZADO: filial → [pessoa + produto] paralelo → conta
    inicio >> trigger_dim_filial >> [trigger_dim_pessoa, trigger_dim_produto]
    [trigger_dim_pessoa, trigger_dim_produto] >> checkpoint >> trigger_dim_conta >> fim
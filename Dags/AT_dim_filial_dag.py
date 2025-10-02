from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import random

def atualizar_dados_dim_filial(ti):
    hook = PostgresHook(postgres_conn_id='postgres_dw_pipeline')
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT cd_filial, nm_filial, nr_cnpj, estado, cidade,
               versao, dt_fundacao
        FROM dw.dim_filial
        WHERE ativo = 'S'
    """)
    filiais = cur.fetchall()

    dados = []
    eventos = []
    cidades_alternativas = ["Goiânia", "Campinas", "Uberlândia", "Niterói", "Joinville"]
    estados_alternativos = ["SP", "RJ", "MG", "RS", "PR"]

    for f in filiais:
        cd_filial, nm_filial, nr_cnpj, estado, cidade, versao, dt_fundacao = f

        alterar = random.random() < 0.8
        encerrar = random.random() < 0.09

        if encerrar:
            cur.execute("""
                UPDATE dw.dim_filial
                SET ativo = 'N', dt_fim = CURRENT_DATE, dt_modificacao = NOW()
                WHERE cd_filial = %s AND ativo = 'S'
            """, (cd_filial,))
            continue

        if alterar:
            nova_cidade = random.choice([c for c in cidades_alternativas if c != cidade])
            novo_estado = random.choice([e for e in estados_alternativos if e != estado])
            novo_nome = nm_filial.strip()

            # Gerar eventos
            if nova_cidade != cidade:
                eventos.append((cd_filial, 'cidade', cidade, nova_cidade))
            if novo_estado != estado:
                eventos.append((cd_filial, 'estado', estado, novo_estado))
            if novo_nome != nm_filial:
                eventos.append((cd_filial, 'nm_filial', nm_filial, novo_nome))

            nova_versao = versao + 1

            dados.append({
                'nm_filial': novo_nome,
                'nr_cnpj': nr_cnpj,
                'cd_filial': cd_filial,
                'chave_natural': None,
                'estado': novo_estado,
                'cidade': nova_cidade,
                'versao': nova_versao,
                'ativo': 'S',
                'dt_fundacao': dt_fundacao,
                'dt_inicio': datetime.today().date(),
                'dt_fim': None,
                'dt_modificacao': None,
            })

    # Inserir eventos na tabela evt.ev_dim_filial
    for ev in eventos:
        cur.execute("""
            INSERT INTO evt.ev_dim_filial (
                cd_filial, campo_alterado, valor_anterior, valor_novo,
                tipo_evento, data_evento, origem
            ) VALUES (%s, %s, %s, %s, 'ALTERACAO', CURRENT_DATE, 'simulador')
        """, ev)

    conn.commit()
    cur.close()
    conn.close()

    ti.xcom_push(key='dados_filial', value=dados)

def inserir_staging_dim_filial(ti):
    hook = PostgresHook(postgres_conn_id='postgres_dw_pipeline')
    conn = hook.get_conn()
    cur = conn.cursor()

    dados = ti.xcom_pull(key='dados_filial', task_ids='atualizar_dados_dim_filial')

    for row in dados:
        cur.execute("""
            INSERT INTO staging.dim_filial_raw (
                nm_filial, nr_cnpj, cd_filial, chave_natural, estado, cidade,
                versao, ativo, dt_fundacao, dt_inicio, dt_fim, dt_modificacao
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['nm_filial'], row['nr_cnpj'], row['cd_filial'], row['chave_natural'],
            row['estado'], row['cidade'], row['versao'], row['ativo'],
            row['dt_fundacao'], row['dt_inicio'], row['dt_fim'], row['dt_modificacao']
        ))

    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id='AT_dim_filial_dag',
    start_date=datetime(2025, 9, 19),
    schedule='@monthly',
    catchup=False,
    tags=['dim', 'filial', 'atualizacao', 'eventos']
) as dag:

    inicio = EmptyOperator(task_id='inicio')

    atualizar = PythonOperator(
        task_id='atualizar_dados_dim_filial',
        python_callable=atualizar_dados_dim_filial
    )

    inserir_staging = PythonOperator(
        task_id='inserir_staging_dim_filial',
        python_callable=inserir_staging_dim_filial
    )

    processar_eventos = SQLExecuteQueryOperator(
        task_id='processar_eventos_filial',
        conn_id='postgres_dw_pipeline',
        sql='ETLs/AT/etl_AT_dim_filial.sql'
    )

    fim = EmptyOperator(task_id='finalizado')

    inicio >> atualizar >> inserir_staging >> processar_eventos >> fim
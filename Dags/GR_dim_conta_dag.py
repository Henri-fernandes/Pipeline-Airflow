from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from faker import Faker
from datetime import datetime, timedelta
import random

def gerar_dados_dim_conta(ti):
    fake = Faker('pt_BR')
    tipos_conta = ['corrente', 'poupança', 'investimento']
    situacoes = ['ativa', 'encerrada', 'bloqueada']

    hook = PostgresHook(postgres_conn_id='postgres_dw_pipeline')
    conn = hook.get_conn()
    cur = conn.cursor()

    # Buscar APENAS pessoas ATIVAS da dimensão
    cur.execute("""
        SELECT DISTINCT ON (cd_pessoa) cd_pessoa, nr_cpf, cd_filial 
        FROM dw.dim_pessoa 
        WHERE ativo = 'S'
        ORDER BY cd_pessoa, versao DESC
    """)
    pessoas = cur.fetchall()

    dados = []
    for pessoa in pessoas:
        cd_pessoa, nr_cpf, cd_filial = pessoa
        tipos_usados = set()

        # Gerar até 2 tipos de conta diferentes por pessoa ATIVA
        for _ in range(random.randint(1, 2)):
            tipo = random.choice([t for t in tipos_conta if t not in tipos_usados])
            tipos_usados.add(tipo)

            cd_conta = f"{cd_pessoa}{tipo[:3].upper()}"  # Ex: 12345COR
            situacao = random.choice(situacoes)

            cpf_titular = random.choice([
                nr_cpf,
                nr_cpf.replace('.', '').replace('-', ''),
                nr_cpf.replace('.', '/').replace('-', '/'),
                nr_cpf.replace('.', '_').replace('-', '_'),
                nr_cpf.replace('.', ' ').replace('-', ' ')
            ])

            dt_abertura = fake.date_time_between(start_date='-5y', end_date='-1y')
            dt_inicio = dt_abertura + timedelta(days=random.randint(0, 100))

            dados.append({
                'cd_conta': cd_conta,
                'chave_natural': None,
                'cpf_titular': cpf_titular,
                'tipo_conta': tipo,
                'situacao': situacao,
                'versao': 1,
                'dt_abertura': dt_abertura,
                'dt_inicio': dt_inicio,
                'dt_fim': None,
                'dt_modificacao': None,
                'cd_pessoa': cd_pessoa,
                'cd_filial': cd_filial
            })

    cur.close()
    conn.close()
    ti.xcom_push(key='dados_conta', value=dados)

def insert_dados_staging_dim_conta(ti):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id='postgres_dw_pipeline')
    conn = hook.get_conn()
    cur = conn.cursor()

    dados = ti.xcom_pull(key='dados_conta', task_ids='gerar_dados_dim_conta')

    for row in dados:
        cur.execute("""
            INSERT INTO staging.dim_conta_raw (
                cd_conta, chave_natural, cpf_titular, tipo_conta, situacao, versao,
                dt_abertura, dt_inicio, dt_fim, dt_modificacao, cd_pessoa, cd_filial
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['cd_conta'], row['chave_natural'], row['cpf_titular'], row['tipo_conta'], row['situacao'],
            row['versao'], row['dt_abertura'], row['dt_inicio'], row['dt_fim'],
            row['dt_modificacao'], row['cd_pessoa'], row['cd_filial']
        ))

    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id='GR_dim_conta_dag',
    start_date=datetime(2025, 9, 23),
    schedule=None,
    catchup=False,
    tags=['dim_conta', 'geracao'],
    description='Geração de dados da dimensão conta - APENAS para pessoas ATIVAS'
) as dag:

    inicio = EmptyOperator(task_id='inicio')

    gerar = PythonOperator(
        task_id='gerar_dados_dim_conta',
        python_callable=gerar_dados_dim_conta
    )

    insert_staging = PythonOperator(
        task_id='insert_staging_dim_conta',
        python_callable=insert_dados_staging_dim_conta
    )

    etl = SQLExecuteQueryOperator(
        task_id='etl_dim_conta_sql',
        conn_id='postgres_dw_pipeline',
        sql='ETLs/GR/etl_GR_dim_conta.sql'
    )

    fim = EmptyOperator(task_id='fim')

    inicio >> gerar >> insert_staging >> etl >> fim
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from faker import Faker
import random

def gerar_dados_ft_investimento(ti):
    fake = Faker('pt_BR')
    hook = PostgresHook(postgres_conn_id='postgres_dw_pipeline')
    conn = hook.get_conn()
    cur = conn.cursor()

    # Busca pessoas ativas
    cur.execute("SELECT cd_pessoa FROM dw.dim_pessoa WHERE ativo = 'S'")
    pessoas = [r[0] for r in cur.fetchall()]

    # Busca produtos da categoria Investimento
    cur.execute("""
        SELECT cd_produto, nm_produto
        FROM dw.dim_produto 
        WHERE categoria = 'Investimento'
    """)
    produtos = cur.fetchall()

    # Busca filiais ativas
    cur.execute("SELECT cd_filial FROM dw.dim_filial WHERE ativo = 'S'")
    filiais = [r[0] for r in cur.fetchall()]

    # Busca contas de investimento válidas
    cur.execute("""
        SELECT cd_conta FROM dw.dim_conta 
        WHERE dt_fim = '9999-12-31'
        AND tipo_conta = 'INVESTIMENTO'
        AND situacao IN ('ATIVA', 'BLOQUEADA')
    """)
    contas = [r[0] for r in cur.fetchall()]

    dados = []

    # Gera 500 registros simulados de aplicações financeiras
    for _ in range(500):
        cd_pessoa = random.choice(pessoas)
        cd_produto, tipo_investimento = random.choice(produtos)
        cd_filial = random.choice(filiais)
        cd_conta = random.choice(contas)

        nr_aplicacao = str(random.randint(100000, 999999))
        dt_aplicacao = fake.date_time_between(start_date='-2y', end_date='now')
        dt_inclusao = datetime.now()

        # Função para simular sujeira nos dados (espaços, vírgulas, etc.)
        def sujar(v):
            v_str = str(v)
            return random.choice([
                f" {v_str} ",
                f"{v_str}  ",
                f"  {v_str}",
                v_str.replace(".", ","),
                v_str
            ])

        vl_aplicado = sujar(f"{random.uniform(500, 100000):.4f}")

        dados.append({
            'nr_aplicacao': sujar(nr_aplicacao),
            'tipo_investimento': sujar(tipo_investimento),
            'vl_aplicado': vl_aplicado,
            'cd_produto': sujar(cd_produto),
            'cd_filial': sujar(cd_filial),
            'cd_pessoa': sujar(cd_pessoa),
            'cd_conta': sujar(cd_conta),
            'dt_aplicacao': dt_aplicacao,
            'dt_inclusao': dt_inclusao,
            'dt_modificacao': None,
            'ativo': 'S'
        })

    # Compartilha os dados com a próxima task via XCom
    ti.xcom_push(key='investimentos_brutos', value=dados)
    cur.close()
    conn.close()

# Insere os dados gerados na tabela staging.ft_investimento_raw
def inserir_staging_ft_investimento(ti):
    hook = PostgresHook(postgres_conn_id='postgres_dw_pipeline')
    conn = hook.get_conn()
    cur = conn.cursor()

    dados = ti.xcom_pull(key='investimentos_brutos', task_ids='gerar_dados_ft_investimento')

    for row in dados:
        cur.execute("""
            INSERT INTO staging.ft_investimento_raw (
                nr_aplicacao, tipo_investimento, vl_aplicado,
                cd_produto, cd_filial, cd_pessoa, cd_conta,
                dt_aplicacao, dt_inclusao, dt_modificacao, ativo
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['nr_aplicacao'], row['tipo_investimento'], row['vl_aplicado'],
            row['cd_produto'], row['cd_filial'], row['cd_pessoa'], row['cd_conta'],
            row['dt_aplicacao'], row['dt_inclusao'], row['dt_modificacao'], row['ativo']
        ))

    conn.commit()
    cur.close()
    conn.close()

# DAG que orquestra a geração de dados sintéticos para a fato investimento
with DAG(
    dag_id='GR_ft_investimento_dag',
    start_date=datetime(2025, 9, 29),
    schedule=None,
    catchup=False,
    tags=['ft_investimento'],
    description='Geração de dados sintéticos para a fato investimento com sujeira simulada'
) as dag:

    inicio = EmptyOperator(task_id='inicio')

    gerar = PythonOperator(
        task_id='gerar_dados_ft_investimento',
        python_callable=gerar_dados_ft_investimento
    )

    inserir_staging = PythonOperator(
        task_id='inserir_staging_ft_investimento',
        python_callable=inserir_staging_ft_investimento
    )

    etl = SQLExecuteQueryOperator(
        task_id='etl_ft_investimento_sql',
        conn_id='postgres_dw_pipeline',
        sql='ETLs/GR/etl_GR_ft_investimento.sql'
    )

    fim = EmptyOperator(task_id='fim')

    inicio >> gerar >> inserir_staging >> etl >> fim

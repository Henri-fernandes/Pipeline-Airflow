from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from faker import Faker
import random

def gerar_dados_ft_credito_liberado(ti):
    fake = Faker('pt_BR')
    hook = PostgresHook(postgres_conn_id='postgres_dw_pipeline')
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("SELECT cd_pessoa FROM dw.dim_pessoa WHERE ativo = 'S'")
    pessoas = [r[0] for r in cur.fetchall()]

    cur.execute("""
        SELECT cd_produto, nm_produto
        FROM dw.dim_produto 
        WHERE categoria = 'Crédito'
    """)
    produtos = cur.fetchall()

    cur.execute("SELECT cd_filial FROM dw.dim_filial WHERE ativo = 'S'")
    filiais = [r[0] for r in cur.fetchall()]

    cur.execute("""
        select * FROM dw.dim_conta 
        WHERE dt_fim = '9999-12-31'
        and tipo_conta in ('CRÉDITO', 'POUPANÇA')
        and situacao in ('ATIVA' , 'BLOQUEADA')
    """)
    contas = [r[0] for r in cur.fetchall()]

    dados = []
    for _ in range(500):
        cd_pessoa = random.choice(pessoas)
        cd_produto, tipo_credito = random.choice(produtos)
        cd_filial = random.choice(filiais)
        cd_conta = random.choice(contas)

        nr_contrato = str(random.randint(100000, 999999))
        dt_contratacao = fake.date_between(start_date='-2y', end_date='today')
        dt_inicio = dt_contratacao + timedelta(days=random.randint(1, 10))
        dt_inclusao = datetime.now()

        def sujar(v):
            v_str = str(v)
            return random.choice([
                f" {v_str} ",
                f"{v_str}  ",
                f"  {v_str}",
                v_str.replace(".", ","),
                v_str
             ])

        vl_credito = sujar(f"{random.uniform(1000, 50000):.4f}")
        vl_juros = sujar(f"{random.uniform(1.5, 20.0):.3f}")
        vl_parcela = sujar(f"{random.uniform(100, 2000):.3f}")
        qtd_parcelas = random.choice(["12", "24", "36", "48", "60", "36.0", "24,0", ""])

        dados.append({
            'nr_contrato': sujar(nr_contrato),
            'vl_credito': vl_credito,
            'vl_juros': vl_juros,
            'vl_parcela': vl_parcela,
            'qtd_parcelas': qtd_parcelas,
            'tipo_credito': sujar(tipo_credito),
            'cd_produto': sujar(cd_produto),
            'cd_filial': sujar(cd_filial),
            'cd_pessoa': sujar(cd_pessoa),
            'cd_conta': sujar(cd_conta),
            'dt_contratacao': dt_contratacao,
            'dt_inicio': dt_inicio,
            'dt_fim': None,
            'dt_inclusao': dt_inclusao,
            'dt_modificacao': None,
            'ativo': 'S'
        })

    ti.xcom_push(key='creditos_brutos', value=dados)
    cur.close()
    conn.close()

def inserir_staging_ft_credito_liberado(ti):
    hook = PostgresHook(postgres_conn_id='postgres_dw_pipeline')
    conn = hook.get_conn()
    cur = conn.cursor()

    dados = ti.xcom_pull(key='creditos_brutos', task_ids='gerar_dados_ft_credito_liberado')

    for row in dados:
        cur.execute("""
            INSERT INTO staging.ft_credito_liberado_raw (
                nr_contrato, vl_credito, vl_juros, vl_parcela,
                qtd_parcelas, tipo_credito, cd_produto, cd_filial, cd_pessoa, cd_conta,
                dt_contratacao, dt_inicio, dt_fim, dt_inclusao, dt_modificacao, ativo
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['nr_contrato'], row['vl_credito'], row['vl_juros'], row['vl_parcela'],
            row['qtd_parcelas'], row['tipo_credito'], row['cd_produto'], row['cd_filial'], row['cd_pessoa'], row['cd_conta'],
            row['dt_contratacao'], row['dt_inicio'], row['dt_fim'], row['dt_inclusao'], row['dt_modificacao'], row['ativo']
        ))

    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id='GR_ft_credito_liberado_dag',
    start_date=datetime(2025, 9, 29),
    schedule=None,
    catchup=False,
    tags=['ft_credito_liberado']
) as dag:

    inicio = EmptyOperator(task_id='inicio')

    gerar = PythonOperator(
        task_id='gerar_dados_ft_credito_liberado',
        python_callable=gerar_dados_ft_credito_liberado
    )

    inserir_staging = PythonOperator(
        task_id='inserir_staging_ft_credito_liberado',
        python_callable=inserir_staging_ft_credito_liberado
    )

    etl = SQLExecuteQueryOperator(
        task_id='etl_ft_credito_liberado_sql',
        conn_id='postgres_dw_pipeline',
        sql='ETLs/GR/etl_GR_ft_credito_liberado.sql'
    )

    fim = EmptyOperator(task_id='fim')

    inicio >> gerar >> inserir_staging >> etl >> fim

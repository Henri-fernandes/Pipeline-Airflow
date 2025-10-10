from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from faker import Faker
import random

def gerar_dados_ft_seguro(ti):
    fake = Faker('pt_BR')
    hook = PostgresHook(postgres_conn_id='postgres_dw_pipeline')
    conn = hook.get_conn()
    cur = conn.cursor()

    # Busca pessoas ativas
    cur.execute("SELECT cd_pessoa FROM dw.dim_pessoa WHERE ativo = 'S'")
    pessoas = [r[0] for r in cur.fetchall()]

    # Busca produtos da categoria Seguro
    cur.execute("""
        SELECT cd_produto, nm_produto
        FROM dw.dim_produto 
        WHERE categoria = 'Seguro'
    """)
    produtos = cur.fetchall()

    # Busca filiais ativas
    cur.execute("SELECT cd_filial FROM dw.dim_filial WHERE ativo = 'S'")
    filiais = [r[0] for r in cur.fetchall()]

    # Busca chaves naturais já existentes para evitar duplicidade
    cur.execute("SELECT chave_natural FROM dw.ft_seguro")
    chaves_existentes = set(r[0] for r in cur.fetchall())

    dados = []
    tentativas = 0

    # Gera até 500 registros únicos com no máximo 2000 tentativas
    while len(dados) < 500 and tentativas < 2000:
        tentativas += 1

        cd_pessoa = random.choice(pessoas)
        cd_produto, tipo_seguro = random.choice(produtos)
        cd_filial = random.choice(filiais)

        nm_apolice = str(random.randint(100000, 999999))
        dt_contratacao = fake.date_time_between(start_date='-2y', end_date='now')
        dt_inicio = dt_contratacao + timedelta(days=random.randint(0, 10))
        dt_inclusao = datetime.now()

        # Função para simular sujeira nos dados (espaços, vírgulas, etc.)
        def sujar(valor):
            return random.choice([
                f" {valor} ",
                f"{valor}  ",
                f"  {valor}",
                valor.replace(".", ","),
                valor
            ])

        vl_premio = sujar(f"{random.uniform(100, 1000):.4f}")
        vl_premio_liquido = sujar(f"{random.uniform(80, 900):.3f}")
        vl_comissao = sujar(f"{random.uniform(1, 50):.3f}")
        vl_cobertura = sujar(f"{random.uniform(10000, 500000):.3f}")
        sinistro = random.choice(["Sim", "Não", "sim", "nao", "NÃO", "True", "False", "S", "N", ""])

        dados.append({
            'nm_apolice': sujar(nm_apolice),
            'vl_premio': vl_premio,
            'vl_premio_liquido': vl_premio_liquido,
            'vl_comissao': vl_comissao,
            'vl_cobertura': vl_cobertura,
            'sinistro': sinistro,
            'tipo_seguro': sujar(tipo_seguro),
            'cd_produto': sujar(cd_produto),
            'cd_filial': sujar(cd_filial),
            'cd_pessoa': sujar(cd_pessoa),
            'dt_contratacao': dt_contratacao,
            'dt_inicio': dt_inicio,
            'dt_fim': None,
            'dt_inclusao': dt_inclusao,
            'dt_modificacao': None,
            'ativo': 'S'
        })

    # Compartilha os dados com a próxima task via XCom
    ti.xcom_push(key='seguros_brutos', value=dados)
    cur.close()
    conn.close()

# Insere os dados gerados na tabela staging.ft_seguro_raw
def inserir_staging_ft_seguro(ti):
    hook = PostgresHook(postgres_conn_id='postgres_dw_pipeline')
    conn = hook.get_conn()
    cur = conn.cursor()

    dados = ti.xcom_pull(key='seguros_brutos', task_ids='gerar_dados_ft_seguro')

    for row in dados:
        cur.execute("""
            INSERT INTO staging.ft_seguro_raw (
                nm_apolice, vl_premio, vl_premio_liquido, vl_comissao,
                vl_cobertura, sinistro, tipo_seguro, cd_produto, cd_filial, cd_pessoa,
                dt_contratacao, dt_inicio, dt_fim, dt_inclusao, dt_modificacao, ativo
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['nm_apolice'], row['vl_premio'], row['vl_premio_liquido'], row['vl_comissao'],
            row['vl_cobertura'], row['sinistro'], row['tipo_seguro'], row['cd_produto'], row['cd_filial'], row['cd_pessoa'],
            row['dt_contratacao'], row['dt_inicio'], row['dt_fim'], row['dt_inclusao'], row['dt_modificacao'], row['ativo']
        ))

    conn.commit()
    cur.close()
    conn.close()

# DAG que orquestra a geração de dados sintéticos para a fato seguro
with DAG(
    dag_id='GR_ft_seguro_dag',
    start_date=datetime(2025, 9, 29),
    schedule=None,
    catchup=False,
    tags=['ft_seguro'],
    description='Geração de dados sintéticos para a fato seguro com sujeira simulada'
) as dag:

    inicio = EmptyOperator(task_id='inicio')

    gerar = PythonOperator(
        task_id='gerar_dados_ft_seguro',
        python_callable=gerar_dados_ft_seguro
    )

    inserir_staging = PythonOperator(
        task_id='inserir_staging_ft_seguro',
        python_callable=inserir_staging_ft_seguro
    )

    etl = SQLExecuteQueryOperator(
        task_id='etl_ft_seguro_sql',
        conn_id='postgres_dw_pipeline',
        sql='ETLs/GR/etl_GR_ft_seguro.sql'
    )

    fim = EmptyOperator(task_id='fim')

    inicio >> gerar >> inserir_staging >> etl >> fim

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from faker import Faker
import random

def gerar_dados_dim_filial(ti):
    fake = Faker('pt_BR')
    capitais_brasil = [
        {"cidade": "Rio Branco", "estado": "AC"}, {"cidade": "Maceió", "estado": "AL"},
        {"cidade": "Macapá", "estado": "AP"}, {"cidade": "Manaus", "estado": "AM"},
        {"cidade": "Salvador", "estado": "BA"}, {"cidade": "Fortaleza", "estado": "CE"},
        {"cidade": "Brasília", "estado": "DF"}, {"cidade": "Vitória", "estado": "ES"},
        {"cidade": "Goiânia", "estado": "GO"}, {"cidade": "São Luís", "estado": "MA"},
        {"cidade": "Cuiabá", "estado": "MT"}, {"cidade": "Campo Grande", "estado": "MS"},
        {"cidade": "Belo Horizonte", "estado": "MG"}, {"cidade": "Belém", "estado": "PA"},
        {"cidade": "João Pessoa", "estado": "PB"}, {"cidade": "Curitiba", "estado": "PR"},
        {"cidade": "Recife", "estado": "PE"}, {"cidade": "Teresina", "estado": "PI"},
        {"cidade": "Rio de Janeiro", "estado": "RJ"}, {"cidade": "Natal", "estado": "RN"},
        {"cidade": "Porto Alegre", "estado": "RS"}, {"cidade": "Porto Velho", "estado": "RO"},
        {"cidade": "Boa Vista", "estado": "RR"}, {"cidade": "Florianópolis", "estado": "SC"},
        {"cidade": "São Paulo", "estado": "SP"}, {"cidade": "Aracaju", "estado": "SE"},
        {"cidade": "Palmas", "estado": "TO"}
    ]
    
    nomes_empresas_financeiras = [
        "Delta Financeiro S.A.",
        "Alpha Investimentos DTVM",
        "Sigma Capital Management",
        "Omega Financial Services",
        "Beta Bank do Brasil",
        "Gamma Corretora de Valores",
        "Zeta Asset Management",
        "Lambda Investimentos S.A.",
        "Theta Banking Solutions",
        "Kappa Credit Union",
        "Epsilon Finance Corp",
        "Rho Securities Brasil",
        "Tau Wealth Management",
        "Phoenix Digital",
        "Nexus Financial Group",
        "Apex Investimentos CCTVM",
        "Vertex Capital Partners",
        "Prime Banking Solutions",
        "Elite Financeira Ltda.",
        "Quantum Investment Bank",
        "Vector Asset Holdings",
        "Matrix Financial Corp",
        "Fusion Bank Brasil",
        "Stellar Investimentos S.A.",
        "Horizon Banking Group",
        "Pinnacle Finance Ltd",
        "Vanguard Capital Brasil",
        "Summit Investment Services",
        "Meridian Digital",
        "Catalyst Financial Group"
    ]

    dados = []
    for i, nome in enumerate(nomes_empresas_financeiras, 1):
        if random.random() < 0.3:
            nome = random.choice([
                nome + " " * random.randint(1, 5),
                " " * random.randint(1, 5) + nome,
                " " * random.randint(1, 3) + nome + " " * random.randint(1, 3),
                nome.replace(" ", "  "),
                f" {nome} . ",
            ])
            
        cnpj_base = fake.cnpj()
        cnpj = random.choice([
            cnpj_base,
            cnpj_base.replace(".", "").replace("-", ""),
            cnpj_base.replace(".", "/").replace("-", "/"),
            cnpj_base.replace(".", "_").replace("-", "_"),
            cnpj_base.replace(".", " ").replace("-", " "),
        ])
        cd_filial = f"{random.randint(1, 99999):05d}"
        local = random.choice(capitais_brasil)
        dt_fundacao = fake.date_between(start_date='-30y', end_date='-1y')
        dt_inicio = fake.date_between(start_date=dt_fundacao, end_date='today')

        dados.append({
            'nm_filial': nome,
            'nr_cnpj': cnpj,
            'cd_filial': cd_filial,
            'chave_natural': None,
            'estado': local["estado"],
            'cidade': local["cidade"],
            'versao': 1,
            'ativo': 'S',
            'dt_fundacao': dt_fundacao,
            'dt_inicio': dt_inicio,
            'dt_fim': None,
            'dt_modificacao': None
        })

    ti.xcom_push(key='dados_filial', value=dados)

def inserir_staging_dim_filial(ti):
    hook = PostgresHook(postgres_conn_id='postgres_dw_pipeline')
    conn = hook.get_conn()
    cur = conn.cursor()

    dados = ti.xcom_pull(key='dados_filial', task_ids='gerar_dados_dim_filial')

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
    dag_id='GR_dim_filial_dag',
    start_date=datetime(2025, 9, 15),
    schedule=None,
    catchup=False,
    tags=['dim_filial']
) as dag:

    inicio = EmptyOperator(task_id='inicio')

    gerar = PythonOperator(
        task_id='gerar_dados_dim_filial',
        python_callable=gerar_dados_dim_filial
    )

    inserir_staging = PythonOperator(
        task_id='inserir_staging_dim_filial',
        python_callable=inserir_staging_dim_filial
    )

    etl = SQLExecuteQueryOperator(
        task_id='etl_dim_filial_sql',
        conn_id='postgres_dw_pipeline',
        sql='ETLs/GR/etl_GR_dim_filial.sql'
    )

    fim = EmptyOperator(task_id='finalizado')

    inicio >> gerar >> inserir_staging >> etl >> fim
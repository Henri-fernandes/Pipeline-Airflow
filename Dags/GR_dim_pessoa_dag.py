from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from faker import Faker
import random

def buscar_filiais_existentes():
    """Busca todas as filiais existentes no banco"""
    hook = PostgresHook(postgres_conn_id='postgres_dw_pipeline')
    conn = hook.get_conn()
    cur = conn.cursor()
    
    cur.execute("SELECT cd_filial FROM dw.dim_filial WHERE ativo = 'S'")
    filiais = [row[0] for row in cur.fetchall()]
    
    cur.close()
    conn.close()
    
    return filiais

def gerar_dados_dim_pessoa(ti):
    fake = Faker('pt_BR')
    capitais_brasil = [
        {"cidade": "Rio Branco", "estado": "Acre", "sigla": "AC"},
        {"cidade": "Maceió", "estado": "Alagoas", "sigla": "AL"},
        {"cidade": "Macapá", "estado": "Amapá", "sigla": "AP"},
        {"cidade": "Manaus", "estado": "Amazonas", "sigla": "AM"},
        {"cidade": "Salvador", "estado": "Bahia", "sigla": "BA"},
        {"cidade": "Fortaleza", "estado": "Ceará", "sigla": "CE"},
        {"cidade": "Brasília", "estado": "Distrito Federal", "sigla": "DF"},
        {"cidade": "Vitória", "estado": "Espírito Santo", "sigla": "ES"},
        {"cidade": "Goiânia", "estado": "Goiás", "sigla": "GO"},
        {"cidade": "São Luís", "estado": "Maranhão", "sigla": "MA"},
        {"cidade": "Cuiabá", "estado": "Mato Grosso", "sigla": "MT"},
        {"cidade": "Campo Grande", "estado": "Mato Grosso do Sul", "sigla": "MS"},
        {"cidade": "Belo Horizonte", "estado": "Minas Gerais", "sigla": "MG"},
        {"cidade": "Belém", "estado": "Pará", "sigla": "PA"},
        {"cidade": "João Pessoa", "estado": "Paraíba", "sigla": "PB"},
        {"cidade": "Curitiba", "estado": "Paraná", "sigla": "PR"},
        {"cidade": "Recife", "estado": "Pernambuco", "sigla": "PE"},
        {"cidade": "Teresina", "estado": "Piauí", "sigla": "PI"},
        {"cidade": "Rio de Janeiro", "estado": "Rio de Janeiro", "sigla": "RJ"},
        {"cidade": "Natal", "estado": "Rio Grande do Norte", "sigla": "RN"},
        {"cidade": "Porto Alegre", "estado": "Rio Grande do Sul", "sigla": "RS"},
        {"cidade": "Porto Velho", "estado": "Rondônia", "sigla": "RO"},
        {"cidade": "Boa Vista", "estado": "Roraima", "sigla": "RR"},
        {"cidade": "Florianópolis", "estado": "Santa Catarina", "sigla": "SC"},
        {"cidade": "São Paulo", "estado": "São Paulo", "sigla": "SP"},
        {"cidade": "Aracaju", "estado": "Sergipe", "sigla": "SE"},
        {"cidade": "Palmas", "estado": "Tocantins", "sigla": "TO"}
    ]

    # Buscar filiais existentes
    filiais_existentes = buscar_filiais_existentes()
    
    if not filiais_existentes:
        raise ValueError("Nenhuma filial encontrada no banco. Verifique a tabela dim_filial.")

    dados = []
    for i in range(1, 1001):
        # Nome da pessoa com chance de espaços extras
        nome = fake.name()
        if random.random() < 0.2:
            opcoes = [
                nome + " " * random.randint(1, 5),
                " " * random.randint(1, 5) + nome,
                " " * random.randint(1, 3) + nome + " " * random.randint(1, 3),
                nome.replace(" ", "  "),
                f" {nome} . ",
            ]
            nome = random.choice(opcoes)

        # CPF com formatos variados
        cpf_base = fake.cpf()
        formatos = [
            cpf_base,  
            cpf_base.replace(".", "").replace("-", ""),  
            cpf_base.replace(".", "/").replace("-", "/"),  
            cpf_base.replace(".", "_").replace("-", "_"),  
            cpf_base.replace(".", " ").replace("-", " "),  
        ]
        cpf = random.choice(formatos)
        cd_pessoa = f"{random.randint(1, 9999999):07d}" 

        # Email com chance de espaços extras
        email = fake.email()
        if random.random() < 0.05:
            opcoes_email = [
                email + " " * random.randint(1, 3),
                " " * random.randint(1, 3) + email,
                " " * random.randint(1, 2) + email + " " * random.randint(1, 2)
            ]
            email = random.choice(opcoes_email)

        # Selecionar filial aleatória das existentes
        cd_filial = random.choice(filiais_existentes)
        contato = fake.phone_number()
        salario = round(random.uniform(1200, 250000), 2)
        local = random.choice(capitais_brasil)
        dt_cadastro = fake.date_time_between(start_date='-5y', end_date='-1y')
        dt_inicio = fake.date_between(start_date=dt_cadastro, end_date='now')

        dados.append({
            'nome_pessoa': nome,
            'nr_cpf': cpf,
            'cd_pessoa': cd_pessoa,
            'chave_natural': None,
            'versao': 1,
            'email': email,
            'contato': contato,
            'salario': salario,
            'cidade': local["cidade"],
            'estado': local["estado"],
            'sigla_estado': local["sigla"],
            'cd_filial': cd_filial,
            'ativo': 'S',
            'dt_cadastro': dt_cadastro,
            'dt_inicio': dt_inicio,
            'dt_fim': None,
            'dt_modificacao': None
        })

    ti.xcom_push(key='dados_pessoa', value=dados)

def insert_dados_staging_dim_pessoa(ti):
    hook = PostgresHook(postgres_conn_id='postgres_dw_pipeline')
    conn = hook.get_conn()
    cur = conn.cursor()

    dados = ti.xcom_pull(key='dados_pessoa', task_ids='gerar_dados_dim_pessoa')

    for row in dados:
        cur.execute("""
            INSERT INTO staging.dim_pessoa_raw (
                nome_pessoa, nr_cpf, cd_pessoa, chave_natural, email, contato, salario, 
                cidade, estado, sigla_estado, cd_filial, versao, ativo, dt_cadastro, dt_inicio, dt_fim, dt_modificacao
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
         """, (
            row['nome_pessoa'], row['nr_cpf'], row['cd_pessoa'], row['chave_natural'], 
            row['email'], row['contato'], row['salario'], row['cidade'], row['estado'], 
            row['sigla_estado'], row['cd_filial'], row['versao'], row['ativo'], row['dt_cadastro'], 
            row['dt_inicio'], row['dt_fim'], row['dt_modificacao']   
        ))

    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id='GR_dim_pessoa_dag',
    start_date=datetime(2025, 9, 22),
    schedule=None,
    catchup=False,
    tags=['dim_pessoa']
) as dag:
    
    inicio = EmptyOperator(task_id='inicio')

    gerar = PythonOperator(
        task_id='gerar_dados_dim_pessoa',
        python_callable=gerar_dados_dim_pessoa
    )

    insert_staging = PythonOperator(
        task_id='insert_staging_dim_pessoa',
        python_callable=insert_dados_staging_dim_pessoa
    )

    etl = SQLExecuteQueryOperator(
        task_id='etl_dim_pessoa_sql',
        conn_id='postgres_dw_pipeline',
        sql='ETLs/GR/etl_GR_dim_pessoa.sql'
    )

    fim = EmptyOperator(task_id='finalizado')

    inicio >> gerar >> insert_staging >> etl >> fim
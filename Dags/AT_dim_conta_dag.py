from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import random

def atualizar_dados_dim_conta(ti):
    hook = PostgresHook(postgres_conn_id='postgres_dw_pipeline')
    conn = hook.get_conn()
    cur = conn.cursor()

    # Pega todas as filiais ativas para usar nas simulações
    cur.execute("SELECT cd_filial FROM dw.dim_filial WHERE ativo = 'S'")
    filiais_validas = [row[0] for row in cur.fetchall()]

    # Busca a versão mais recente de cada conta junto com os dados da pessoa
    cur.execute("""
        SELECT DISTINCT ON (c.cd_conta)
            c.cd_conta, c.cd_pessoa, c.cd_filial, c.tipo_conta, c.situacao,
            c.versao, c.dt_abertura, p.nr_cpf, p.ativo
        FROM dw.dim_conta c
        JOIN dw.dim_pessoa p ON c.cd_pessoa = p.cd_pessoa
        ORDER BY c.cd_conta, c.versao DESC
    """)
    contas = cur.fetchall()

    # Identifica pessoas ativas que ainda não têm conta
    cur.execute("""
        SELECT DISTINCT ON (p.cd_pessoa) 
            p.cd_pessoa, p.nr_cpf, p.cd_filial
        FROM dw.dim_pessoa p
        WHERE p.ativo = 'S'
          AND NOT EXISTS (
              SELECT 1 FROM dw.dim_conta c 
              WHERE c.cd_pessoa = p.cd_pessoa
          )
        ORDER BY p.cd_pessoa, p.versao DESC
    """)
    pessoas_sem_conta = cur.fetchall()

    dados = []     # Aqui vamos guardar os registros que serão inseridos na staging
    eventos = []   # Aqui ficam os eventos que serão registrados na tabela de auditoria

    # Para cada conta, aplicamos as regras de negócio
    for c in contas:
        cd_conta, cd_pessoa, cd_filial, tipo_conta, situacao, versao, dt_abertura, nr_cpf, pessoa_ativa = c
        nova_versao = versao + 1

        # Se a pessoa está inativa e a conta ainda não foi encerrada, encerramos agora
        if pessoa_ativa == 'N' and situacao.upper() != 'ENCERRADA':
            dados.append({
                'cd_conta': cd_conta,
                'chave_natural': None,
                'cpf_titular': nr_cpf,
                'tipo_conta': tipo_conta,
                'situacao': 'ENCERRADA',
                'versao': nova_versao,
                'dt_abertura': dt_abertura,
                'dt_inicio': datetime.today().date(),
                'dt_fim': datetime.today().date(),
                'dt_modificacao': datetime.now(),
                'cd_pessoa': cd_pessoa,
                'cd_filial': cd_filial
            })
            eventos.append((cd_conta, 'situacao', situacao, 'ENCERRADA'))
            continue

        # Se a conta está ativa, existe uma chance de 10% de trocar a filial
        if pessoa_ativa == 'S' and situacao.upper() != 'ENCERRADA':
            trocar_filial = random.random() < 0.1
            if trocar_filial:
                nova_filial = random.choice([f for f in filiais_validas if f != cd_filial])
                dados.append({
                    'cd_conta': cd_conta,
                    'chave_natural': None,
                    'cpf_titular': nr_cpf,
                    'tipo_conta': tipo_conta,
                    'situacao': situacao,
                    'versao': nova_versao,
                    'dt_abertura': dt_abertura,
                    'dt_inicio': datetime.today().date(),
                    'dt_fim': None,
                    'dt_modificacao': None,
                    'cd_pessoa': cd_pessoa,
                    'cd_filial': nova_filial
                })
                eventos.append((cd_conta, 'cd_filial', cd_filial, nova_filial))

    # Para cada pessoa ativa sem conta, criamos uma conta nova
    for p in pessoas_sem_conta:
        cd_pessoa, nr_cpf, cd_filial = p
        cd_conta = f"{cd_pessoa}COR"

        dados.append({
            'cd_conta': cd_conta,
            'chave_natural': None,
            'cpf_titular': nr_cpf,
            'tipo_conta': 'corrente',
            'situacao': 'ATIVA',
            'versao': 1,
            'dt_abertura': datetime.today().date(),
            'dt_inicio': datetime.today().date(),
            'dt_fim': None,
            'dt_modificacao': None,
            'cd_pessoa': cd_pessoa,
            'cd_filial': cd_filial
        })

    # Registra os eventos de alteração na tabela de auditoria
    for ev in eventos:
        cur.execute("""
            INSERT INTO evt.ev_dim_conta (
                cd_conta, campo_alterado, valor_anterior, valor_novo,
                tipo_evento, data_evento, origem
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, ev + ('ALTERACAO', datetime.now(), 'PYTHON'))

    conn.commit()
    cur.close()
    conn.close()

    # Compartilha os dados com a próxima task via XCom
    ti.xcom_push(key='dados_conta', value=dados)

# Essa função insere os dados gerados na camada staging para posterior processamento
def inserir_staging_dim_conta(ti):
    hook = PostgresHook(postgres_conn_id='postgres_dw_pipeline')
    conn = hook.get_conn()
    cur = conn.cursor()

    dados = ti.xcom_pull(key='dados_conta', task_ids='atualizar_dados_dim_conta')

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

# DAG que orquestra o processo de atualização da dimensão conta
with DAG(
    dag_id='AT_dim_conta_dag',
    start_date=datetime(2025, 9, 23),
    schedule='@monthly',
    catchup=False,
    tags=['dim', 'conta', 'atualizacao'],
    description='Atualiza a dimensão conta com base em regras de negócio simuladas'
) as dag:

    inicio = EmptyOperator(task_id='inicio')

    atualizar = PythonOperator(
        task_id='atualizar_dados_dim_conta',
        python_callable=atualizar_dados_dim_conta
    )

    inserir_staging = PythonOperator(
        task_id='inserir_staging_dim_conta',
        python_callable=inserir_staging_dim_conta
    )

    etl_dim_conta = SQLExecuteQueryOperator(
        task_id='etl_dim_conta_sql',
        conn_id='postgres_dw_pipeline',
        sql='ETLs/AT/etl_AT_dim_conta.sql'
    )

    fim = EmptyOperator(task_id='finalizado')

    inicio >> atualizar >> inserir_staging >> etl_dim_conta >> fim

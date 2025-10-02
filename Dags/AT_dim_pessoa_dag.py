from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import random

def atualizar_dim_pessoa(ti):
    hook = PostgresHook(postgres_conn_id='postgres_dw_pipeline')
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(""" 
        SELECT cd_pessoa, nome_pessoa, nr_cpf, estado, cidade, versao, dt_cadastro
        FROM dw.dim_pessoa
        WHERE ativo = 'S'
    """)
    pessoas = cur.fetchall()

    dados = []
    eventos = []
    cidades_alternativas = ["Goiânia", "Campinas", "Uberlândia", "Niterói", "Joinville"]
    estados_alternativos = ["SP", "RJ", "MG", "RS", "PR"]

    for pessoa in pessoas:
        cd_pessoa, nome_pessoa, nr_cpf, estado, cidade, versao, dt_cadastro = pessoa

        alterar = random.random() < 0.8
        encerrar = random.random() < 0.09 

        if encerrar:
            cur.execute(""" 
                UPDATE dw.dim_pessoa
                SET ativo = 'N', dt_fim = CURRENT_DATE, dt_modificacao = NOW()
                WHERE cd_pessoa = %s AND ativo = 'S'
            """, (cd_pessoa,))
            continue 

        if alterar:
            nova_cidade = random.choice([c for c in cidades_alternativas if c != cidade])
            novo_estado = random.choice([e for e in estados_alternativos if e != estado])
            novo_nome = nome_pessoa.strip()

            # Gerar eventos
            if nova_cidade != cidade:
                eventos.append((cd_pessoa, 'cidade', cidade, nova_cidade))
            if novo_estado != estado:
                eventos.append((cd_pessoa, 'estado', estado, novo_estado))
            if novo_nome != nome_pessoa:
                eventos.append((cd_pessoa, 'nome_pessoa', nome_pessoa, novo_nome))

            nova_versao = versao + 1
        
            dados.append({
                'nome_pessoa': novo_nome,
                'nr_cpf': nr_cpf,
                'cd_pessoa': cd_pessoa,
                'chave_natural': None,
                'estado': novo_estado,
                'cidade': nova_cidade,
                'versao': nova_versao,
                'ativo': 'S',
                'dt_cadastro': dt_cadastro,
                'dt_inicio': datetime.today().date(),
                'dt_fim': None,
                'dt_modificacao': None,
            })

    # Inserir eventos na tabela evt.ev_dim_pessoa
    for ev in eventos:
        cur.execute("""
            INSERT INTO evt.ev_dim_pessoa (
                cd_pessoa, campo_alterado, valor_anterior, valor_novo,
                tipo_evento, data_evento, origem
            ) VALUES (%s, %s, %s, %s, 'ALTERACAO', CURRENT_DATE, 'simulador')
        """, ev)

    conn.commit()
    cur.close()
    conn.close()

    ti.xcom_push(key='dados_pessoa', value=dados)

def insert_staging_dim_pessoa(ti):
    hook = PostgresHook(postgres_conn_id='postgres_dw_pipeline')
    conn = hook.get_conn()
    cur = conn.cursor()

    dados = ti.xcom_pull(key='dados_pessoa', task_ids='atualizar_dados_dim_pessoa')

    for row in dados:
        cur.execute(""" 
            INSERT INTO staging.dim_pessoa_raw (
                nome_pessoa, nr_cpf, cd_pessoa, chave_natural, estado, cidade,
                versao, ativo, dt_cadastro, dt_inicio, dt_fim, dt_modificacao
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (  
            row['nome_pessoa'], row['nr_cpf'], row['cd_pessoa'], row['chave_natural'],
            row['estado'], row['cidade'], row['versao'], row['ativo'],
            row['dt_cadastro'], row['dt_inicio'], row['dt_fim'], row['dt_modificacao']
        ))
    
    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id='AT_dim_pessoa_dag',
    start_date=datetime(2025, 9, 22),
    schedule='@monthly',
    catchup=False,
    tags=['dim_pessoa', 'eventos']
) as dag:
    
    inicio = EmptyOperator(task_id='inicio')

    atualizar = PythonOperator(
        task_id='atualizar_dados_dim_pessoa',
        python_callable=atualizar_dim_pessoa
    )

    inserir_staging = PythonOperator(
        task_id='inserir_staging_dim_pessoa',
        python_callable=insert_staging_dim_pessoa
    )

    processar_eventos = SQLExecuteQueryOperator(
        task_id='processar_eventos_pessoa',
        conn_id='postgres_dw_pipeline',
        sql='ETLs/AT/etl_AT_dim_pessoa.sql'
    )

    fim = EmptyOperator(task_id='finalizado')

    inicio >> atualizar >> inserir_staging >> processar_eventos >> fim
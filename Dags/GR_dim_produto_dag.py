from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from faker import Faker
import random

def gerar_dados_dim_produto(ti):
    fake = Faker('pt_BR')

    produtos_categoria = {
        "Conta Corrente": "Conta",
        "Conta Poupança": "Conta",
        "Conta Digital": "Conta",
        "Cartão de Crédito": "Crédito",
        "Cartão Pré-Pago": "Crédito",
        "Cartão Empresarial": "Crédito",
        "Empréstimo Pessoal": "Crédito",
        "Empréstimo Consignado": "Crédito",
        "Crédito Imobiliário": "Crédito",
        "Crédito Veicular": "Crédito",
        "CDB": "Investimento",
        "LCI": "Investimento",
        "LCA": "Investimento",
        "Tesouro Direto": "Investimento",
        "Fundos de Ações": "Investimento",
        "Fundos Multimercado": "Investimento",
        "Previdência Privada": "Investimento",
        "Seguro de Vida": "Seguro",
        "Seguro Residencial": "Seguro",
        "Seguro Veicular": "Seguro",
        "Seguro Viagem": "Seguro",
        "Pix": "Pagamento",
        "Boleto Bancário": "Pagamento",
        "Transferência TED": "Pagamento",
        "Débito Automático": "Pagamento"
    }

    dados = []

    for i, nome_base in enumerate(produtos_categoria.keys(), start=1):
        cd_produto = f"{random.randint(1, 99999999):08d}"

        nome = nome_base
        if random.random() < 0.5:
            nome = random.choice([
                nome + " " * random.randint(1, 5),
                " " * random.randint(1, 5) + nome,
                " " * random.randint(1, 3) + nome + " " * random.randint(1, 3),
                nome.replace(" ", "  "),
                f" {nome} "
            ])

        categoria = produtos_categoria[nome_base]
        descricao = f"{nome.strip()} - Produto da categoria {categoria.lower()} oferecido pela instituição financeira."
        
        # Converter para string para evitar problemas de serialização
        dt_inicio_obj = fake.date_between(start_date='-5y', end_date='today')
        dt_inicio = dt_inicio_obj.strftime('%Y-%m-%d')

        dados.append({
            'cd_produto': cd_produto,
            'nm_produto': nome,
            'categoria': categoria,
            'descricao': descricao,
            'versao': 1,
            'dt_inicio': dt_inicio,
            'dt_fim': None,
            'dt_modificacao': None
        })

    # Log para debug
    print(f"Dados gerados: {len(dados)} registros")
    print(f"Exemplo do primeiro registro: {dados[0] if dados else 'Nenhum dado gerado'}")
    
    ti.xcom_push(key='dados_produto', value=dados)

def inserir_staging_dim_produto(ti):
    hook = PostgresHook(postgres_conn_id='postgres_dw_pipeline')
    conn = hook.get_conn()
    cur = conn.cursor()

    # Recuperar dados do XCom
    dados = ti.xcom_pull(key='dados_produto', task_ids='gerar_dados_dim_produto')
    
    # Log para debug
    print(f"Dados recuperados do XCom: {len(dados) if dados else 0} registros")
    if dados:
        print(f"Chaves do primeiro registro: {list(dados[0].keys())}")
        print(f"Primeiro registro completo: {dados[0]}")

    # Verificar se dados foram recuperados
    if not dados:
        raise ValueError("Nenhum dado foi recuperado do XCom")

    # Inserir dados no staging
    for i, row in enumerate(dados):
        try:
            # Verificar se todas as chaves necessárias estão presentes
            required_keys = ['cd_produto', 'nm_produto', 'categoria', 'descricao', 'versao', 'dt_inicio', 'dt_fim', 'dt_modificacao']
            missing_keys = [key for key in required_keys if key not in row]
            
            if missing_keys:
                raise KeyError(f"Chaves ausentes no registro {i}: {missing_keys}")
            
            cur.execute("""
                INSERT INTO staging.dim_produto_raw (
                    cd_produto, nm_produto, categoria, descricao, versao,
                    dt_inicio, dt_fim, dt_modificacao
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['cd_produto'], 
                row['nm_produto'], 
                row['categoria'], 
                row['descricao'], 
                row['versao'],
                row['dt_inicio'], 
                row['dt_fim'], 
                row['dt_modificacao']
            ))
            
        except Exception as e:
            print(f"Erro ao inserir registro {i}: {e}")
            print(f"Conteúdo do registro: {row}")
            raise

    conn.commit()
    cur.close()
    conn.close()
    
    print(f"Inseridos {len(dados)} registros com sucesso")

with DAG(
    dag_id='GR_dim_produto_dag',
    start_date=datetime(2025, 9, 24),
    schedule=None,
    catchup=False,
    tags=['dim_produto']
) as dag:

    inicio = EmptyOperator(task_id='inicio')

    gerar = PythonOperator(
        task_id='gerar_dados_dim_produto',
        python_callable=gerar_dados_dim_produto
    )

    inserir_staging = PythonOperator(
        task_id='inserir_staging_dim_produto',
        python_callable=inserir_staging_dim_produto
    )

    etl = SQLExecuteQueryOperator(
        task_id='etl_dim_produto_sql',
        conn_id='postgres_dw_pipeline',
        sql='ETLs/GR/etl_GR_dim_produto.sql'
    )

    fim = EmptyOperator(task_id='fim')

    inicio >> gerar >> inserir_staging >> etl >> fim
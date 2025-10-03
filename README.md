![Status](https://img.shields.io/badge/Status-Concluído-brightgreen)

# 🚀 Pipeline de Dados com Apache Airflow

<div align="center">

![Airflow](https://img.shields.io/badge/Apache%20Airflow-19A119?style=for-the-badge&logo=Apache%20Airflow&logoColor=black)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=306998)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
## Pipeline ETL robusto para geração, transformação e carga de dados no Data Warehouse

</div>

# **📋 Sumário**

### [🎯 Visão Geral](#-visão-geral)
### [🌀 Fluxo do Pipeline](#fluxo-do-pipeline)
### [💡 Por Que Este Projeto Existe?](#-por-que-este-projeto-existe)
### [🎯 Impacto no Negócio](#-impacto-no-negócio)
### [🔄 Diferença Entre DAGs AT vs GR](#-diferença-entre-dags-at-vs-gr)
### [🔑 Conceito de UNIQUE (Chave Natural + Versão)](#-conceito-de-unique--chave-natural--versão-)
### [🛠️ Tecnologias e Justificativas](#️-tecnologias-e-justificativas)
### [🔍 Por Que Cada Ferramenta?](#-por-que-cada-ferramenta)
### [🖋️ Scripts das Tabelas](#️scripts-das-tabelas)
### [🌀 Fluxo de Versionamento](#fluxo-de-versionamento)
### [📏 Regras de Negócio Implementadas](#regras-de-negócio-implementadas)
### [🎯 Boas Práticas](#-boas-práticas)
### [🔧 Instalação](#-instalação)
### [🔧 Como Executar](#-como-executar)
### [📸 Exemplos Visuais](#-exemplos-visuais)
### [⚙️ Troubleshooting](#️troubleshooting)
### [📧 Contato](#-contato)
---

# **🎯 Visão Geral**


>Este projeto implementa um **pipeline de dados end-to-end** utilizando **Apache Airflow** como orquestrador, projetado para simular um ambiente de produção real de Data Warehousing. O pipeline gerencia todo o ciclo de vida dos dados: desde a geração de dados sintéticos realistas até a carga final em um Data Warehouse dimensional, mantendo histórico completo e garantindo integridade referencial.

### 🛠️ **Tecnologias Utilizadas**

>**Python + Faker**: Geração de dados sintéticos realistas (pessoas, Crédito, contas, etc.)
>
>**Apache Airflow**: Orquestração e automação de todo o fluxo de dados, executando as tarefas em sequência >e horários programados
>
>**PostgreSQL**: Banco de dados relacional que armazena tanto a camada de staging quanto o Data Warehouse >final
>
>**SQL**: Transformações, validações e aplicação de regras de negócio nos dados
>
>### ✨ **Benefícios**
>
>✅ Ambiente seguro para testes sem usar dados reais  
>✅ Automação completa via Airflow  
>✅ Dados realistas para aprendizado e desenvolvimento  
>✅ Arquitetura escalável e profissional
---


# **🌀Fluxo do Pipeline:**

>### 1️⃣ **Geração de Dados** 🎲
>Dados sintéticos são criados usando Python com Faker para simular informações realistas em ambientes de >desenvolvimento e teste.
>
>### 2️⃣ **Staging** 📥
>Os dados brutos chegam na área temporária `staging.dim_pessoa_raw`, onde ficam armazenados sem >transformações, prontos para processamento.
>
>### 3️⃣ **Transformação** 🧹
>Scripts SQL aplicam limpeza, validação e regras de negócio. Dados inconsistentes são tratados e apenas >informações de qualidade seguem adiante.
>
>### 4️⃣ **Data Warehouse** 🏛️
>Dados validados são armazenados em `dw.dim_pessoa` de forma estruturada e otimizada para análises, >servindo como fonte única da verdade.
---

# **💡 Por Que Este Projeto Existe?**

### **Problemas Que Resolve**


>| 🔴 Problema | ✅ Solução Implementada |
>|------------|------------------------|
>| **Dados duplicados no DW** | Sistema de versionamento com chave natural composta ( Chave_produto + versão) |
>| **Falta de dados para testes** | Geração automática de dados sintéticos realistas com Faker |
>| **Perda de histórico de alterações** | Arquitetura SCD Type 2 com controle temporal completo |
>| **Pipeline manual e propenso a erros** | Orquestração automática via Airflow com retry e monitoramento |
>| **Inconsistência entre ambientes** | Docker Compose para ambiente replicável e isolado |
>| **Difícil rastreabilidade** | Logging detalhado e metadados em todas as camadas |
---

# **🎯 Impacto no Negócio**


>### Para o Time de Dados
>- ⚡ **Redução de 70% no tempo de desenvolvimento** de novos pipelines
>- 🛡️ **Zero duplicatas** no DW graças ao controle de versão e boas práticas
>- 🔍 **Rastreabilidade completa** de todas as transformações
>
>>### Para o Time de BI/Analytics
>- 📊 **Dados confiáveis** para dashboards e relatórios
>- 📈 **Histórico completo** para análises de tendências temporais
>- ✓ **Validação prévia** de regras de negócio antes da produção
>
>>### Para a Empresa
>- 💰 **Economia de recursos** com detecção precoce de erros
>- 🚀 **Time-to-market reduzido** para novos casos de uso
>- 🎓 **Ambiente de aprendizado** para novos membros do time
 

---

# **🔄 Diferença Entre DAGs AT vs GR**

### As tabelas de Dimensões utilizam dois tipos de DAGs com propósitos distintos:


>| Aspecto | 🔵 DAG **AT** | 🟢 DAG **GR** |
>|---------|--------------------------------|-------------------------|
>| **Propósito** | Atualizar os dados existentes do Data Warehouse | Gerar os dados que iram para o Data Warehouse |
>| **Dependências** |  Dependências dos Dags GR | Tarefas encadeadas com dependências de outros GR |
>| **Exemplo Real** | `AT_dim_pessoa_dag.py` | `GR_dim_pessoa_dag.py` |
>| **Reusabilidade** | Alta (componente isolado) | Único (fluxo específico) |



## **📌 Exemplo Prático de DAGs**

### **🔵 DAG AT:**
```python
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
```

### **🟢 DAG GR:**
```python
def gerar_dados_dim_pessoa(ti):
    fake = Faker('pt_BR')
    capitais_brasil = ["cidade": "Rio Branco", "estado": "Acre", "sigla": "AC"] # Lista Simplificada P/ Exemplo

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
```

# 🔑 Conceito de UNIQUE ( Chave Natural + Versão )

### Uma das boas práticas deste pipeline é o uso de **Unique** na **Chave Natural** e **Versão** para garantir unicidade e rastreabilidade:

```sql
-- Estrutura da chave composta
SELECT
p.cd_pessoa || '|' || REGEXP_REPLACE(p.nr_cpf, '[^0-9]', '', 'g') AS chave_natural
FROM staging.dim_pessoa_raw

--- Estrutura d0 UNIQUE

CONSTRAINT unq_nr_cpf_versao_cd_pessoa UNIQUE (nr_cpf, versao, cd_pessoa)

-- Exemplo Funcional:

-- CPF         | Versão | CD Pessoa |     Nome      |    Chave Natural    | Data Carga
-- 12345678901 |   1    | 7654321   | João Silva    | 7654321|12345678901 | 2024-01-15
-- 12345678901 |   2    | 7654321   | João Silva Jr | 7654321|12345678901 | 2024-02-20
-- 98765432100 |   1    | 8765432   | Maria Santos  | 8765432|98765432100 | 2024-01-15


-- Exemplo NÃO Funcional: UNIQUE NÃO Permite ter nr_cpf, versao, cd_pessoa iguais em mais de uma linha, Evitando duplicidade.

-- CPF         | Versão | CD Pessoa |     Nome      |    Chave Natural    | Data Carga
-- 12345678901 |   1    | 7654321   | João Silva    | 7654321|12345678901 | 2024-01-15  
-- 12345678901 |   1    | 7654321   | João Silva Jr | 7654321|12345678901 | 2024-02-20
-- 98765432100 |   1    | 8765432   | Maria Santos  | 8765432|98765432100 | 2024-01-15


```

## **Por que isso é importante?**
```
- ✅ Permite rastrear **todas as mudanças** de uma pessoa ao longo do tempo
- ✅ Evita **duplicatas acidentais** durante cargas
- ✅ Facilita **auditorias** e análises históricas
- ✅ Implementa **SCD Type 2** de forma nativa

```

---

# 🛠️ Tecnologias e Justificativas

### ⚙️ Stack Tecnológico

>| Tecnologia | Versão | Justificativa de Escolha |
>|-----------|--------|-------------------------|
>| **Apache Airflow** | 3.x | Orquestração robusta com UI intuitiva, retry automático, monitoramento e agendamento flexível |
>| **Python** | 3.9+ | Linguagem versátil com vasto ecossistema de bibliotecas para manipulação de dados |
>| **PostgreSQL** | 14+ | Banco ACID-compliant, suporte a transações complexas, queries analíticas eficientes |
>| **Faker** | 18.x | Geração de dados sintéticos realistas para testes e desenvolvimento |
>| **Docker** | 20.x | Isolamento de ambiente, reprodutibilidade e facilidade de deploy |
>| **Docker Compose** | 2.x | Orquestração multi-container simplificada |

# 🔍 Por Que Cada Ferramenta?

### Apache Airflow
```python
# Vantagens do Airflow neste projeto:
✅ Agendamento cron nativo (@daily, @hourly, custom)
✅ Retry automático em caso de falhas
✅ Monitoramento visual via UI
✅ Paralelização de tarefas
✅ Logging centralizado
✅ Versionamento de DAGs via Git
✅ Framework de orquestração baseado em Python
```

### PostgreSQL
```python
# Recursos críticos utilizados:
✅ Constraints (PRIMARY KEY, FOREIGN KEY, UNIQUE)
✅ CTEs (Common Table Expressions)
✅ Indexes para performance
```

### Faker
```python
# Exemplo de geração de dados realistas
from faker import Faker
fake = Faker('pt_BR')

pessoa = {
    'cpf': fake.cpf(),                    # 123.456.789-01
    'nome': fake.name(),                  # João Silva Santos
    'email': fake.email(),                # joao.silva@example.com
    'tipo_conta': fake.random_element(['corrente', 'poupanca', 'salario'])
}
```
---


# **🖋️Scripts das Tabelas**

### **Tabela de staging:** `dim_pessoa_raw`

```sql
CREATE TABLE staging.dim_pessoa_raw (
    nome_pessoa     TEXT,
    cd_pessoa       TEXT,
    nr_cpf          TEXT,
    chave_natural   TEXT,
    email           TEXT,
    contato         TEXT,
    salario         TEXT,
    estado          TEXT,
    cidade			TEXT,
    sigla_estado    TEXT,
    cd_filial       TEXT,
    versao          INT,
    ativo           TEXT,
    dt_cadastro     TIMESTAMP,
    dt_inicio       TIMESTAMP,
    dt_fim          TIMESTAMP,
    dt_modificacao  TIMESTAMP
);
```

>### **Características:**
>- ✅ Recebe dados brutos do sistema fonte
>- ✅ Timestamp de carga para auditoria

### **Tabela do DW:** `dim_pessoa`

```sql
CREATE TABLE dw.dim_pessoa (
    id_pessoa SERIAL PRIMARY KEY,       
    nome_pessoa varchar (100) not null,
    cd_pessoa char (7) not null,
    nr_cpf char(11) not null,
    chave_natural char (19) not null,
    email varchar(100),
    contato varchar(20),
    salario numeric(10,2) not null,
    cidade varchar(50) not null,
    estado varchar(50) not null,        
    sigla_estado char(2) not null,
    cd_filial char (5) not null,
    versao int not null,                
    ativo CHAR(1) DEFAULT 'S' CHECK (ativo IN ('S','N')),
    dt_cadastro date not null,
    dt_inicio date not null,
    dt_fim date not null default '9999-12-31',
    dt_modificacao timestamp not null default now(),
    dt_referencia date not null,
    CONSTRAINT chk_datas CHECK (dt_fim >= dt_inicio),
    CONSTRAINT unq_nr_cpf_versao_cd_pessoa UNIQUE (nr_cpf, versao, cd_pessoa)
)

create index idx_dim_pessoa_cidade
	on dw.dim_pessoa (cidade)
	
create index idx_dim_pessoa_estado 
	on dw.dim_pessoa (estado)
	
create index idx_dim_pessoa_salario 
	on dw.dim_pessoa (salario)
	
create index idx_dim_cd_pessoa 
	on dw.dim_pessoa (cd_pessoa)
```

>### **Características:**
>- ✅ Keys para joins eficientes
>- ✅ Mantém histórico completo (SCD Type 2)
>- ✅ Controle de vigência temporal
>- ✅ Flag de registro ativo para facilitar queries

# **🌀Fluxo de Versionamento**

>### **Como Funciona o Controle de Versões**
>
>### O pipeline implementa um sistema cuidadoso de versionamento que mantém o **histórico completo de alterações** nos dados, permitindo análises temporais e auditoria.
>
>---
>
>## 📝 Exemplo Prático: Mudança de Tipo de Conta
>
>### **Versão 1 - Registro Inicial**
>
>1. **Sistema Fonte** envia os dados: João Silva com Conta corrente 12345 e Filial 00001 
>2. **Staging** recebe e armazena como **versão 1** com timestamp
>3. **ETL** processa e valida os dados
>4. **Data Warehouse** insere o registro (verifica se não existe pelo `CD_CONTA`)
>
>### **Resultado**: João Silva (Conta corrente 12345, v1, Filial 00001 ) → **ATIVO**
>
>---
>
>### **Versão 2 - Atualização de Dados**
>
>### Cenário: João muda de Filial
>1. **Sistema Fonte** envia os novos dados: João Silva com Conta corrente 12345 e Filial 00002
>2. **Staging** recebe e armazena como **versão 2** com novo timestamp
>3. **ETL** identifica que já existe um registro para o Conta corrente 12345
>4. **Data Warehouse** executa duas operações:
>   - **Insere** a nova versão 2 com os dados atualizados
>   - **Atualiza** a versão 1: marca `ativo = 'N'` e define `dt_fim`
>
>### **Resultado no Data Warehouse**:
>- ### João Silva (Conta corrente 12345, v1, Filial 00001) → **INATIVO** (histórico)
>- ###  João Silva (Conta corrente 12345, v2, Filial 00002) → **ATIVO** (atual)
>
>---
>
>## 🎯 Benefícios do Versionamento
>
>✅ **Histórico Completo**: Todas as alterações são preservadas  
>✅ **Auditoria**: Saber exatamente quando e o que mudou  
>✅ **Análise Temporal**: Consultar como os dados estavam em qualquer período  
>✅ **Recuperação**: Possibilidade de reverter para versões anteriores  
>✅ **Conformidade**: Atende requisitos regulatórios de rastreabilidade
>
>## 🔑 Campos de Controle
>
>- **Versão**: Número sequencial identificando cada alteração
>- **flag_ativo**: Indica se é a versão atual (S) ou histórica (N)
>- **data_inicio_vigencia**: Quando a versão foi criada
>- **data_fim_vigencia**: Quando a versão foi substituída (NULL se ainda ativa)



# **📏Regras de Negócio Implementadas**

### 1.  **ETL** simplificado da geração dos dados

```sql
-- 1. Verifica se não existe informações desta pessoa antes do INSERT
INSERT INTO dw.dim_pessoa (
    nome_pessoa, cd_pessoa, nr_cpf, chave_natural, email, cd_filial, versao, ativo,
    dt_cadastro, dt_inicio, dt_fim, dt_modificacao
)
SELECT
    UPPER(TRIM(REPLACE(REPLACE(REPLACE(p.nome_pessoa, '  ', ' '), '.', ''), '  ', ' '))) AS nome_pessoa,
    p.cd_pessoa,
    REGEXP_REPLACE(p.nr_cpf, '[^0-9]', '', 'g') AS nr_cpf,
    p.cd_pessoa || '|' || REGEXP_REPLACE(p.nr_cpf, '[^0-9]', '', 'g') AS chave_natural,
    TRIM(p.email),
    p.cd_filial,
    p.versao,
    UPPER(p.ativo),
    p.dt_cadastro,
    p.dt_inicio,
    COALESCE(p.dt_fim, DATE '9999-12-31'),
    COALESCE(p.dt_modificacao, CURRENT_TIMESTAMP)
FROM staging.dim_pessoa_raw p
WHERE NOT EXISTS ( 
    SELECT 1
    FROM dw.dim_pessoa d
    WHERE d.cd_pessoa = p.cd_pessoa
      AND d.nr_cpf = REGEXP_REPLACE(p.nr_cpf, '[^0-9]', '', 'g')
      AND d.versao = p.versao
);


```

### 2. **ETL** simplificado da Atualização de Versões 

```sql
-- Quando uma nova versão chega, inativa as versões anteriores
-- 1. Identificar versão ativa de cada pessoa
WITH versoes_ativas AS (
    SELECT *
    FROM dw.dim_pessoa
    WHERE ativo = 'S'
),
-- 2. Buscar eventos mais recentes por campo
eventos AS (
    SELECT DISTINCT ON (cd_pessoa, campo_alterado)
           cd_pessoa, campo_alterado, valor_novo, data_evento
    FROM evt.ev_dim_pessoa
    WHERE tipo_evento = 'ALTERACAO'
    ORDER BY cd_pessoa, campo_alterado, data_evento DESC
),
-- 3. Juntar eventos com versão ativa
eventos_por_campo AS (
    SELECT
        va.cd_pessoa,
        va.nome_pessoa,
        va.nr_cpf,
        va.email,
        va.cd_filial,
        va.versao,
        va.dt_cadastro,
        ev_nome.valor_novo AS novo_nome,
        ev_email.valor_novo AS novo_email,
        ev_filial.valor_novo AS nova_filial,
        GREATEST(
            COALESCE(ev_nome.data_evento, DATE '1900-01-01'),
            COALESCE(ev_email.data_evento, DATE '1900-01-01'),
            COALESCE(ev_filial.data_evento, DATE '1900-01-01')
        ) AS maior_data_evento
    FROM versoes_ativas va
    LEFT JOIN eventos ev_nome   ON va.cd_pessoa = ev_nome.cd_pessoa AND ev_nome.campo_alterado = 'nome_pessoa'
    LEFT JOIN eventos ev_email  ON va.cd_pessoa = ev_email.cd_pessoa AND ev_email.campo_alterado = 'email'
    LEFT JOIN eventos ev_filial  ON va.cd_pessoa = ev_filial.cd_pessoa AND ev_filial.campo_alterado = 'cd_filial'
),
-- 4. Pessoas que precisam ser atualizadas
pessoas_a_atualizar AS (
    SELECT *
    FROM eventos_por_campo
    WHERE
        COALESCE(novo_nome, nome_pessoa) <> nome_pessoa OR
        COALESCE(novo_email, email) <> email OR
        COALESCE(nova_filial, cd_filial) <> cd_filial
),
-- 5. Encerrar versão atual
encerrar AS (
    UPDATE dw.dim_pessoa d
    SET ativo = 'N',
        dt_fim = CURRENT_DATE,
        dt_modificacao = CURRENT_TIMESTAMP
    FROM pessoas_a_atualizar p
    WHERE d.cd_pessoa = p.cd_pessoa AND d.ativo = 'S'
    RETURNING d.*
)
-- 6. Inserir nova versão
INSERT INTO dw.dim_pessoa (
    nome_pessoa, cd_pessoa, nr_cpf, chave_natural, email, cd_filial, versao, ativo,
    dt_cadastro, dt_inicio, dt_fim, dt_modificacao
)
SELECT
    UPPER(COALESCE(p.novo_nome, p.nome_pessoa)),
    p.cd_pessoa,
    p.nr_cpf,
    UPPER(p.cd_pessoa || '|' || p.nr_cpf),
    COALESCE(p.novo_email, p.email),
    UPPER(COALESCE(p.nova_filial, p.cd_filial)),
    p.versao + 1,
    'S',
    p.dt_cadastro,
    CURRENT_DATE,
    DATE '9999-12-31',
    CURRENT_TIMESTAMP
FROM pessoas_a_atualizar p;
```

---

# **🎯 Boas Práticas**

### **1. Naming Conventions**

### Padrão de Nomenclatura de Tabelas

```sql
-- ❌ EVITE (nomes genéricos e confusos)
CREATE TABLE pessoa;
CREATE TABLE dados_pessoa;
CREATE TABLE tbl_pessoa;

-- ✅ RECOMENDADO (clareza sobre camada e conteúdo)
CREATE TABLE staging.dim_pessoa_raw;
CREATE TABLE dw.dim_pessoa;
CREATE TABLE dw.ft_investimento;
```

### Convenção utilizada

>- `raw` - Tabelas de staging (dados brutos)
>- `dim` - Dimensões do DW
>- `ft`  - Fatos do DW
>- `ev` - Tabelas da evt(Tabela de eventos)

### Padrão de Nomenclatura Para DAGs

```python

# ✅ DAGs Atômicas (AT)
AT_dim_pessoa_dag
AT_dim_conta_dag
AT_dim_filial_dag

# ✅ DAGs Agrupadas (GR)
GR_dim_pessoa_dag
GR_ft_investimento_dag
GR_ft_seguro_dag

```

### Padrão de Nomenclatura e tipo de dados para as Colunas 

```sql
--- Exemplo
dt_cadastro timestamp 
dt_inicio timestamp 
dt_fim timestamp 
dt_modificacao timestamp 
dt_referencia timestamp 
```

### **2. Versionamento e Chaves Naturais**

### Uso de tabelas para resgistrar eventos

```sql
INSERT INTO evt.ev_dim_pessoa (
                cd_pessoa, campo_alterado, valor_anterior, valor_novo,
                tipo_evento, data_evento, origem
)
```

### Garantia de Unicidade

```sql
CONSTRAINT unq_nr_cpf_versao_cd_pessoa UNIQUE (nr_cpf, versao, cd_pessoa)
```
### **3. Otimização de Performance**

### Índices Estratégicos

```sql
-- ✅ Índices para queries frequentes
create index idx_dim_pessoa_cidade
	on dw.dim_pessoa (cidade)
	
create index idx_dim_pessoa_estado 
	on dw.dim_pessoa (estado)
	
create index idx_dim_pessoa_salario 
	on dw.dim_pessoa (salario)
	
create index idx_dim_cd_pessoa 
	on dw.dim_pessoa (cd_pessoa)
```

# 🔧 **Instalação**

## **Pré-requisitos**
>### - Docker & Docker Compose
>### - Python 3.9+
>### - 4GB RAM disponível
---


## **Documentação para instalação**

> ### 🔗Link da documentação do Airflow: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html?utm_source=chatgpt.com
> ### 🔗Link da documentação do Docker Desktop para Windows: https://docs.docker.com/desktop/setup/install/windows-install/
> ### 🔗Link da documentação do Postgres: https://www.postgresql.org/docs/
> ### 🔗Link da documentação do Python: https://www.python.org/downloads/
> ### 🔗Link da documentação do DBeaver: https://dbeaver.io/download/


## **Clonar Repositório**

```bash
# 1. Clone o repositório
git clone https://github.com/Henri-fernandes/Pipeline-Airflow.git
cd airflow-pipeline

# 2. Configure as variáveis de ambiente
cp .env.example .env

# 3. Suba os containers
docker-compose up -d

# 4. Acesse o Airflow
# URL: http://localhost:8080
# User: admin
# Pass: admin
```

# 🔧 **Como Executar**

### Este guia apresenta o passo a passo para configurar o ambiente, executar o pipeline de dados e verificar os resultados.

## Passo 1: Clonar o Repositório

### Primeiro, você precisa fazer o clone do repositório do projeto para o seu ambiente local. Utilize o comando `git clone` seguido da URL do repositório.

```bash
git clone https://github.com/Henri-fernandes/Pipeline-Airflow.git
```

## Passo 2: Configurar o Banco de Dados
>
>### Após clonar o repositório, localize os scripts de criação de tabelas que estão incluídos no >projeto. Execute esses scripts no seu banco de dados para criar a estrutura necessária das >tabelas. Esses scripts vão preparar o ambiente de dados para receber as informações processadas >pelo Airflow.
>
>### Os scripts das tabelas geralmente estão localizados em uma pasta como `Estrutura/`  dentro do repositório.

---
## Passo 3: Acessar o Airflow
> 
> ### Abra a interface web do Airflow através do navegador. Você será direcionado para o painel  principal da ferramenta, onde poderá visualizar e gerenciar todas as DAGs disponíveis.
> 
---
## Passo 4: Localizar a DAG
>
>### No painel do Airflow, siga estas etapas:
>
>### 1. Clique na aba **"DAGs"** localizada no menu superior
>### 2. Utilize a barra de pesquisa para encontrar a DAG específica chamada `GR_dim_filial_dag`
>### 3. Clique sobre a DAG para visualizar seus detalhes
---
 ## Passo 5: Executar e Acompanhar
> 
> ### Ative a DAG caso ainda não esteja ativa (utilize o botão toggle ao lado do nome da DAG). O Airflow processará todas as tarefas (tasks) definidas nessa DAG automaticamente. Acompanhe o > progresso da execução através da interface gráfica.
---
## Passo 6: Verificar os Resultados
> 
> ### Quando a execução finalizar, observe se todas as tasks estão com o status **"success"** (sucesso), geralmente indicado pela cor verde. 
>
> ### Se todas as tasks estiverem com esse status, significa que o processo foi concluído com êxito. Agora você pode acessar o banco de dados diretamente para verificar os dados que foram inseridos ou atualizados pelas tarefas executadas.
>

---
>### **Observação:** Em caso de falhas, verifique os logs de cada task clicando sobre ela para identificar e corrigir possíveis problemas.
---

# **📸 Exemplos Visuais**

## 🌀 Airflow UI - DAGs 

Onde vai ficar a foto

## 🌀 Airflow UI - Fluxo 

Onde vai ficar a foto


## 📊 Estrutura do Data Warehouse

Onde vai ficar a foto

## 📊 Tabela `dw.dim_filial`

Onde vai ficar a foto

## 🆗Execução

onde video vai ficar

---

# ⚙️Troubleshooting

### Este documento apresenta os problemas mais comuns que podem ocorrer durante a execução do pipeline, suas causas e as respectivas soluções.

## 1. Erro de Conexão com o Banco de Dados

### Mensagem de Erro
```python
# psycopg2.OperationalError: could not connect to server
```

### Causa
>A conexão com o banco de dados está mal configurada no Airflow. Isso pode acontecer quando as credenciais ou parâmetros de conexão estão incorretos ou ausentes.

### Solução
>Verifique se a conexão `postgres_dw_pipeline` está corretamente definida na interface do Airflow:
>
>1. Acesse o Airflow UI
>2. Vá em **Admin** → **Connections**
>3. Localize a conexão `postgres_dw_pipeline`
>4. Confirme se os seguintes parâmetros estão corretos:
>   - **Host**: Endereço do servidor do banco de dados
>   - **Port**: Porta de conexão (geralmente 5432 para PostgreSQL)
>   - **Schema**: Nome do schema/database
>   - **Login**: Usuário do banco de dados
>   - **Password**: Senha do usuário

---

## 2. DAG Não Aparece no Airflow

### Mensagem de Erro

```python
# A DAG não é listada na interface do Airflow.
```

### Causa
>Este problema pode ocorrer por dois motivos principais:
>- O arquivo `.py` da DAG não está na pasta `dags/`
>- Existem erros de sintaxe no código Python da DAG

### Solução
>Siga estas etapas para resolver o problema:
>
>1. **Verifique a localização do arquivo**: Confirme que o arquivo Python está dentro da pasta `dags/` do Airflow
>2. **Verifique erros de sintaxe**: Procure por:
>   - Erros de indentação
>   - Importações quebradas ou módulos não instalados
>   - Erros de sintaxe no código Python
>3. **Consulte os logs**: Verifique os logs do Airflow scheduler para identificar mensagens de erro específicas
>4. **Reinicie o scheduler**: Após corrigir os problemas, reinicie o Airflow scheduler

---

## 4. Task Falha por Falta de XCom

### Mensagem de Erro
```python
# NoneType object has no attribute...
```
ou
```python
# ti.xcom_pull() retorna vazio
```

### Causa
>A task anterior não foi executada com sucesso ou não realizou o `xcom_push` dos dados necessários. Isso quebra a comunicação entre tasks que dependem de dados compartilhados.

### Solução
>Para resolver este problema, siga estas etapas:
>1. **Verifique a ordem das tasks**: Confirme que as dependências estão corretamente configuradas:
   ```python
   # task_1 >> task_2 >> task_3
   ```

>2. **Confirme o xcom_push**: Certifique-se de que a task anterior está enviando os dados:
>3. **Verifique o xcom_pull**: Garanta que a chave está correta ao recuperar os dados:
>4. **Consulte os logs**: Verifique os logs da task anterior para confirmar se ela foi executada com sucesso
>5. **Teste com valores padrão**: Adicione tratamento para casos onde o XCom esteja vazio:

---

## Dicas Gerais de Troubleshooting

>- Sempre consulte os **logs das tasks** no Airflow UI clicando sobre a task específica
>- Utilize o modo **Debug** para identificar problemas em desenvolvimento
>- Verifique as **conexões e variáveis** configuradas no Airflow Admin
>- Teste as queries SQL **diretamente no banco** antes de executar via Airflow
>- Mantenha os **logs detalhados** habilitados durante o desenvolvimento
>- Caso aparece outro erro ou as dicas não funcionem, fique a vontade para pesquisar na internet


# 📧 **Contato**

>**Henrico Fernandes**
> - [LinkedIn](https://www.linkedin.com/in/henricofernandes/) 
> - [Portfólio](https://henri-fernandes.github.io/Portf-lio-Profissional-/)
>- Gmail: fernandes.henri2020@gmail.com


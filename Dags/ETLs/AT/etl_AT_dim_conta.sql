-- ETL SIMPLIFICADO DIM_CONTA
-- 1. Encerrar contas de pessoas inativas (apenas se não estiver já encerrada)
-- 2. Criar contas para pessoas ativas sem conta

-- 1. ENCERRAR CONTAS DE PESSOAS INATIVAS
WITH pessoas_inativas AS (
    -- Buscar pessoas inativas na última versão
    SELECT DISTINCT ON (cd_pessoa) 
        cd_pessoa, ativo
    FROM dw.dim_pessoa 
    ORDER BY cd_pessoa, versao DESC
),
contas_para_encerrar AS (
    -- Buscar contas ativas de pessoas inativas na última versão da conta
    SELECT DISTINCT ON (c.cd_conta) 
        c.cd_conta, c.cd_pessoa, c.cd_filial, c.tipo_conta, c.situacao, 
        c.versao, c.dt_abertura, c.cpf_titular
    FROM dw.dim_conta c
    JOIN pessoas_inativas p ON c.cd_pessoa = p.cd_pessoa
    WHERE p.ativo = 'N' 
      AND UPPER(c.situacao) != 'ENCERRADA'
    ORDER BY c.cd_conta, c.versao DESC
)
INSERT INTO dw.dim_conta (
    cd_conta, chave_natural, cpf_titular, tipo_conta, situacao, versao,
    dt_abertura, dt_inicio, dt_fim, dt_modificacao, cd_pessoa, cd_filial
)
SELECT
    c.cd_conta,
    c.cd_conta || '|' || UPPER(TRIM(c.tipo_conta)) || '|' || c.cd_filial AS chave_natural,
    REGEXP_REPLACE(c.cpf_titular, '[^0-9]', '', 'g') AS cpf_titular,
    UPPER(TRIM(c.tipo_conta)),
    'ENCERRADA',
    c.versao + 1,
    c.dt_abertura,
    CURRENT_DATE,
    DATE '9999-12-31',
    CURRENT_TIMESTAMP,
    c.cd_pessoa,
    c.cd_filial
FROM contas_para_encerrar c;

-- 2. CRIAR CONTAS PARA PESSOAS ATIVAS SEM CONTA
WITH pessoas_ativas AS (
    -- Buscar pessoas ativas na última versão
    SELECT DISTINCT ON (cd_pessoa) 
        cd_pessoa, nr_cpf, cd_filial, ativo
    FROM dw.dim_pessoa 
    WHERE ativo = 'S'
    ORDER BY cd_pessoa, versao DESC
),
pessoas_sem_conta AS (
    -- Pessoas ativas que não têm nenhuma conta
    SELECT p.cd_pessoa, p.nr_cpf, p.cd_filial
    FROM pessoas_ativas p
    WHERE NOT EXISTS (
        SELECT 1 
        FROM dw.dim_conta c 
        WHERE c.cd_pessoa = p.cd_pessoa
    )
)
INSERT INTO dw.dim_conta (
    cd_conta, chave_natural, cpf_titular, tipo_conta, situacao, versao,
    dt_abertura, dt_inicio, dt_fim, dt_modificacao, cd_pessoa, cd_filial
)
SELECT
    p.cd_pessoa || 'COR' AS cd_conta,
    p.cd_pessoa || 'COR|CORRENTE|' || p.cd_filial AS chave_natural,
    REGEXP_REPLACE(p.nr_cpf, '[^0-9]', '', 'g') AS cpf_titular,
    'CORRENTE',
    'ATIVA',
    1,
    CURRENT_DATE,
    CURRENT_DATE,
    DATE '9999-12-31',
    CURRENT_TIMESTAMP,
    p.cd_pessoa,
    p.cd_filial
FROM pessoas_sem_conta p;
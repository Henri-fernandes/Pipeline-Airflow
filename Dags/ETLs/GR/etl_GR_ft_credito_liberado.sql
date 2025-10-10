INSERT INTO dw.ft_credito_liberado (
    chave_natural,       -- Identificador único para evitar duplicidade
    nr_contrato,         -- Número do contrato de crédito
    vl_credito,          -- Valor total do crédito liberado
    vl_juros,            -- Taxa de juros aplicada
    vl_parcela,          -- Valor da parcela mensal
    qtd_parcelas,        -- Quantidade de parcelas
    tipo_credito,        -- Tipo do crédito (ex: pessoal, consignado)
    cd_produto,          -- Código do produto financeiro
    cd_filial,           -- Código da filial onde foi contratado
    cd_pessoa,           -- Código da pessoa contratante
    cd_conta,            -- Código da conta vinculada
    dt_contratacao,      -- Data da contratação do crédito
    dt_inicio,           -- Data de início do contrato
    dt_fim,              -- Data de fim do contrato (default: 9999-12-31)
    dt_inclusao,         -- Data de inclusão no sistema
    dt_modificacao,      -- Data da última modificação
    ativo                -- Indicador de ativo ('S' ou 'N')
)
SELECT
    TRIM(s.nr_contrato) || '|' || TO_CHAR(s.dt_contratacao, 'YYYY-MM-DD') || '|' || TRIM(s.cd_filial) || '|' || TRIM(s.cd_pessoa) || '|' || TO_CHAR(COALESCE(s.dt_inclusao, NOW()), 'YYYY-MM-DD'), -- Chave Natural
    TRIM(s.nr_contrato),                                         -- Limpa espaços do número do contrato
    -- Converte vírgula para ponto e arredonda valores numéricos
    ROUND(REPLACE(s.vl_credito, ',', '.')::numeric, 2),          -- Valor do crédito
    ROUND(REPLACE(s.vl_juros, ',', '.')::numeric, 2),            -- Taxa de juros
    ROUND(REPLACE(s.vl_parcela, ',', '.')::numeric, 2),          -- Valor da parcela

    -- Trata quantidade de parcelas: aceita inteiro ou decimal com vírgula/ponto
    CASE
        WHEN s.qtd_parcelas ~ '^[0-9]+$' THEN s.qtd_parcelas::int
        WHEN s.qtd_parcelas ~ '^[0-9]+[.,][0-9]+$' THEN ROUND(REPLACE(s.qtd_parcelas, ',', '.')::numeric)::int
        ELSE 0                                                   -- Se vier vazio ou inválido, assume 0
    END,

    TRIM(s.tipo_credito),                                        -- Limpa espaços do tipo de crédito
    TRIM(s.cd_produto),                                          -- Limpa espaços do código do produto
    TRIM(s.cd_filial),                                           -- Limpa espaços do código da filial
    TRIM(s.cd_pessoa),                                           -- Limpa espaços do código da pessoa
    TRIM(s.cd_conta),                                            -- Limpa espaços do código da conta

    s.dt_contratacao,                                            -- Mantém data original da contratação
    s.dt_inicio,                                                 -- Mantém data original de início

    COALESCE(s.dt_fim, '9999-12-31'::date),                      -- Preenche fim com data padrão se nulo
    COALESCE(s.dt_inclusao, NOW()),                              -- Preenche inclusão com NOW se nulo
    COALESCE(s.dt_modificacao, NOW()),                           -- Preenche modificação com NOW se nulo

    -- Normaliza campo ativo para 'S' ou 'N'
    CASE
        WHEN LOWER(TRIM(s.ativo)) IN ('s', 'sim', 'true') THEN 'S'
        ELSE 'N'
    END

-- Origem dos dados: staging
FROM staging.ft_credito_liberado_raw s

-- Filtro para evitar duplicidade com base na chave natural
WHERE NOT EXISTS (
    SELECT 1 FROM dw.ft_credito_liberado f
    WHERE f.chave_natural = (
        TRIM(s.nr_contrato) || '|' ||
        TO_CHAR(s.dt_contratacao, 'YYYY-MM-DD') || '|' ||
        TRIM(s.cd_filial) || '|' ||
        TRIM(s.cd_pessoa) || '|' ||
        TO_CHAR(COALESCE(s.dt_inclusao, NOW()), 'YYYY-MM-DD')
    )
)

INSERT INTO dw.ft_seguro (
    chave_natural,               -- Identificador único composto para evitar duplicidade
    nm_apolice,                  -- Número da apólice
    vl_premio,                   -- Valor bruto do prêmio
    vl_premio_liquido,           -- Valor líquido do prêmio
    vl_comissao,                 -- Valor da comissão
    vl_cobertura,                -- Valor da cobertura contratada
    sinistro,                    -- Indicador de ocorrência de sinistro ('S' ou 'N')
    tipo_seguro,                 -- Tipo do seguro (ex: vida, residencial)
    cd_produto,                  -- Código do produto relacionado
    cd_filial,                   -- Código da filial onde foi contratado
    cd_pessoa,                   -- Código da pessoa segurada
    dt_contratacao,              -- Data da contratação do seguro
    dt_inicio,                   -- Data de início da vigência
    dt_fim,                      -- Data de fim da vigência (default: 9999-12-31)
    dt_inclusao,                 -- Data de inclusão no sistema
    dt_modificacao,             -- Data da última modificação (default: NOW)
    ativo                        -- Indicador de ativo ('S' ou 'N')
)
SELECT
    TRIM(s.nm_apolice) || '|' ||                             -- Remove espaços e concatena
    TO_CHAR(s.dt_inclusao, 'YYYY-MM-DD') || '|' ||           -- Formata data de inclusão
    TRIM(s.cd_filial) || '|' ||                              -- Remove espaços da filial
    TO_CHAR(s.dt_contratacao, 'YYYY-MM-DD'),                 -- Formata data de contratação
    TRIM(s.nm_apolice),                                      -- Limpa espaços do número da apólice
    ROUND(REPLACE(s.vl_premio, ',', '.')::numeric, 2),       -- Converte vírgula para ponto e arredonda o prêmio
    ROUND(REPLACE(s.vl_premio_liquido, ',', '.')::numeric, 2), -- Idem para prêmio líquido
    ROUND(REPLACE(s.vl_comissao, ',', '.')::numeric, 2),     -- Idem para comissão
    ROUND(REPLACE(s.vl_cobertura, ',', '.')::numeric, 2),    -- Idem para cobertura
    CASE                                                     -- Normaliza campo sinistro
        WHEN LOWER(TRIM(s.sinistro)) IN ('sim', 'true', 's') THEN 'S'
        WHEN LOWER(TRIM(s.sinistro)) IN ('não', 'nao', 'false', 'n') THEN 'N'
        ELSE 'N'
    END,
    TRIM(s.tipo_seguro),                                     -- Limpa espaços do tipo de seguro
    TRIM(s.cd_produto),                                      -- Limpa espaços do código do produto
    TRIM(s.cd_filial),                                       -- Limpa espaços do código da filial
    TRIM(s.cd_pessoa),                                       -- Limpa espaços do código da pessoa
    s.dt_contratacao,                                        -- Mantém data original da contratação
    s.dt_inicio,                                             -- Mantém data original de início
    COALESCE(s.dt_fim, '9999-12-31'::timestamp),             -- Preenche fim com data padrão se nulo
    COALESCE(s.dt_inclusao, NOW()),                          -- Preenche inclusão com NOW se nulo
    COALESCE(s.dt_modificacao, NOW()),                       -- Preenche modificação com NOW se nulo
    CASE                                                     -- Normaliza campo ativo
        WHEN LOWER(TRIM(s.ativo)) IN ('s', 'sim', 'true') THEN 'S'
        ELSE 'N'
    END
FROM staging.ft_seguro_raw s
-- Evita duplicidade só insere se chave_natural ainda não existe
WHERE NOT EXISTS (
    SELECT 1 FROM dw.ft_seguro f
    WHERE f.chave_natural = (
        TRIM(s.nm_apolice) || '|' ||
        TO_CHAR(s.dt_inclusao, 'YYYY-MM-DD') || '|' ||
        TRIM(s.cd_filial) || '|' ||
        TO_CHAR(s.dt_contratacao, 'YYYY-MM-DD')
    )
)

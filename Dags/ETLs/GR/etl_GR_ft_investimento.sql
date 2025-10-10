INSERT INTO dw.ft_investimento (
    chave_natural,               -- Identificador único para evitar duplicidade
    nr_aplicacao,                -- Número da aplicação
    tipo_investimento,           -- Tipo do investimento (ex: CDB, Tesouro Direto)
    vl_aplicado,                 -- Valor aplicado pelo cliente
    vl_rendimento,              -- Rendimento obtido (se houver)
    cd_produto,                  -- Código do produto financeiro
    cd_filial,                   -- Código da filial onde foi feito o investimento
    cd_pessoa,                   -- Código da pessoa que investiu
    cd_conta,                    -- Código da conta vinculada
    dt_aplicacao,                -- Data da aplicação
    dt_inclusao,                 -- Data de inclusão no sistema
    dt_modificacao,              -- Data da última modificação
    ativo                        -- Indicador de ativo ('S' ou 'N')
)
SELECT
    -- Gera a chave natural composta para controle de duplicidade
    TRIM(s.nr_aplicacao) || '|' || TRIM(s.cd_filial) || '|' || TO_CHAR(s.dt_aplicacao, 'YYYY-MM-DD') || '|' ||                  TRIM(s.cd_produto) || '|' || TO_CHAR(COALESCE(s.dt_inclusao, NOW()), 'YYYY-MM-DD'), -- Chave Natural
    TRIM(s.nr_aplicacao),                                      -- Limpa espaços do número da aplicação
    TRIM(s.tipo_investimento),                                 -- Limpa espaços do tipo de investimento
    -- Converte vírgula para ponto e arredonda valor aplicado
    ROUND(REPLACE(s.vl_aplicado, ',', '.')::numeric, 2),
    -- Trata rendimento: se estiver preenchido, converte e arredonda; senão, deixa nulo
    CASE
        WHEN s.vl_rendimento IS NOT NULL AND s.vl_rendimento <> ''
        THEN ROUND(REPLACE(s.vl_rendimento, ',', '.')::numeric, 2)
        ELSE NULL
    END,
    TRIM(s.cd_produto),                                        -- Limpa espaços do código do produto
    TRIM(s.cd_filial),                                         -- Limpa espaços do código da filial
    TRIM(s.cd_pessoa),                                         -- Limpa espaços do código da pessoa
    TRIM(s.cd_conta),                                          -- Limpa espaços do código da conta
    s.dt_aplicacao,                                            -- Mantém data original da aplicação
    COALESCE(s.dt_inclusao, NOW()),                            -- Preenche data de inclusão com NOW se nula
    COALESCE(s.dt_modificacao, NOW()),                         -- Preenche data de modificação com NOW se nula
    -- Normaliza campo ativo para 'S' ou 'N'
    CASE
        WHEN LOWER(TRIM(s.ativo)) IN ('s', 'sim', 'true') THEN 'S'
        ELSE 'N'
    END
FROM staging.ft_investimento_raw s
-- Filtro para evitar duplicidade com base na chave natural
WHERE NOT EXISTS (
    SELECT 1 FROM dw.ft_investimento f
    WHERE f.chave_natural = (
        TRIM(s.nr_aplicacao) || '|' ||
        TRIM(s.cd_filial) || '|' ||
        TO_CHAR(s.dt_aplicacao, 'YYYY-MM-DD') || '|' ||
        TRIM(s.cd_produto) || '|' ||
        TO_CHAR(COALESCE(s.dt_inclusao, NOW()), 'YYYY-MM-DD')
    )
)

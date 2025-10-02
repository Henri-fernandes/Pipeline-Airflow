INSERT INTO dw.ft_investimento (
    chave_natural, nr_aplicacao, tipo_investimento, vl_aplicado, vl_rendimento,
    cd_produto, cd_filial, cd_pessoa, cd_conta,
    dt_aplicacao, dt_inclusao, dt_modificacao, ativo
)
SELECT
    TRIM(s.nr_aplicacao) || '|' ||
    TRIM(s.cd_filial) || '|' ||
    TO_CHAR(s.dt_aplicacao, 'YYYY-MM-DD') || '|' ||
    TRIM(s.cd_produto) || '|' ||
    TO_CHAR(COALESCE(s.dt_inclusao, NOW()), 'YYYY-MM-DD'),

    TRIM(s.nr_aplicacao),
    TRIM(s.tipo_investimento),
    ROUND(REPLACE(s.vl_aplicado, ',', '.')::numeric, 2),
    CASE
        WHEN s.vl_rendimento IS NOT NULL AND s.vl_rendimento <> ''
        THEN ROUND(REPLACE(s.vl_rendimento, ',', '.')::numeric, 2)
        ELSE NULL
    END,
    TRIM(s.cd_produto),
    TRIM(s.cd_filial),
    TRIM(s.cd_pessoa),
    TRIM(s.cd_conta),
    s.dt_aplicacao,
    COALESCE(s.dt_inclusao, NOW()),
    COALESCE(s.dt_modificacao, NOW()),
    CASE
        WHEN LOWER(TRIM(s.ativo)) IN ('s', 'sim', 'true') THEN 'S'
        ELSE 'N'
    END
FROM staging.ft_investimento_raw s
WHERE NOT EXISTS (
    SELECT 1 FROM dw.ft_investimento f
    WHERE f.chave_natural = (
        TRIM(s.nr_aplicacao) || '|' ||
        TRIM(s.cd_filial) || '|' ||
        TO_CHAR(s.dt_aplicacao, 'YYYY-MM-DD') || '|' ||
        TRIM(s.cd_produto) || '|' ||
        TO_CHAR(COALESCE(s.dt_inclusao, NOW()), 'YYYY-MM-DD')
    )
);

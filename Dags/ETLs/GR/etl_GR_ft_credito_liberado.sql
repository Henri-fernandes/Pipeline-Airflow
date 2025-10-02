INSERT INTO dw.ft_credito_liberado (
    chave_natural, nr_contrato, vl_credito, vl_juros, vl_parcela,
    qtd_parcelas, tipo_credito, cd_produto, cd_filial, cd_pessoa, cd_conta,
    dt_contratacao, dt_inicio, dt_fim, dt_inclusao, dt_modificacao, ativo
)
SELECT
    TRIM(s.nr_contrato) || '|' ||
    TO_CHAR(s.dt_contratacao, 'YYYY-MM-DD') || '|' ||
    TRIM(s.cd_filial) || '|' ||
    TRIM(s.cd_pessoa) || '|' ||
    TO_CHAR(COALESCE(s.dt_inclusao, NOW()), 'YYYY-MM-DD'),
    TRIM(s.nr_contrato),
    ROUND(REPLACE(s.vl_credito, ',', '.')::numeric, 2),
    ROUND(REPLACE(s.vl_juros, ',', '.')::numeric, 2),
    ROUND(REPLACE(s.vl_parcela, ',', '.')::numeric, 2),
    CASE
        WHEN s.qtd_parcelas ~ '^[0-9]+$' THEN s.qtd_parcelas::int
        WHEN s.qtd_parcelas ~ '^[0-9]+[.,][0-9]+$' THEN ROUND(REPLACE(s.qtd_parcelas, ',', '.')::numeric)::int
        ELSE 0
    END,
    TRIM(s.tipo_credito),
    TRIM(s.cd_produto),
    TRIM(s.cd_filial),
    TRIM(s.cd_pessoa),
    TRIM(s.cd_conta),
    s.dt_contratacao,
    s.dt_inicio,
    COALESCE(s.dt_fim, '9999-12-31'::date),
    COALESCE(s.dt_inclusao, NOW()),
    COALESCE(s.dt_modificacao, NOW()),
    CASE
        WHEN LOWER(TRIM(s.ativo)) IN ('s', 'sim', 'true') THEN 'S'
        ELSE 'N'
    END
FROM staging.ft_credito_liberado_raw s
WHERE NOT EXISTS (
    SELECT 1 FROM dw.ft_credito_liberado f
    WHERE f.chave_natural = (
        TRIM(s.nr_contrato) || '|' ||
        TO_CHAR(s.dt_contratacao, 'YYYY-MM-DD') || '|' ||
        TRIM(s.cd_filial) || '|' ||
        TRIM(s.cd_pessoa) || '|' ||
        TO_CHAR(COALESCE(s.dt_inclusao, NOW()), 'YYYY-MM-DD')
    )
);

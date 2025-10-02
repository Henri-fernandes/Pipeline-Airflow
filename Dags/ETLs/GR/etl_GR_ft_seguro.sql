INSERT INTO dw.ft_seguro (
    chave_natural, nm_apolice, vl_premio, vl_premio_liquido, vl_comissao,
    vl_cobertura, sinistro, tipo_seguro, cd_produto, cd_filial, cd_pessoa,
    dt_contratacao, dt_inicio, dt_fim, dt_inclusao, dt_modificacao, ativo
)
SELECT
    TRIM(s.nm_apolice) || '|' ||
    TO_CHAR(s.dt_inclusao, 'YYYY-MM-DD') || '|' ||
    TRIM(s.cd_filial) || '|' ||
    TO_CHAR(s.dt_contratacao, 'YYYY-MM-DD'),
    TRIM(s.nm_apolice),
    ROUND(REPLACE(s.vl_premio, ',', '.')::numeric, 2),
    ROUND(REPLACE(s.vl_premio_liquido, ',', '.')::numeric, 2),
    ROUND(REPLACE(s.vl_comissao, ',', '.')::numeric, 2),
    ROUND(REPLACE(s.vl_cobertura, ',', '.')::numeric, 2),
    CASE
        WHEN LOWER(TRIM(s.sinistro)) IN ('sim', 'true', 's') THEN 'S'
        WHEN LOWER(TRIM(s.sinistro)) IN ('não', 'nao', 'false', 'n') THEN 'N'
        ELSE 'N'
    END,
    TRIM(s.tipo_seguro),
    TRIM(s.cd_produto),
    TRIM(s.cd_filial),
    TRIM(s.cd_pessoa),
    s.dt_contratacao,
    s.dt_inicio,
    COALESCE(s.dt_fim, '9999-12-31'::timestamp),
    COALESCE(s.dt_inclusao, NOW()),
    COALESCE(s.dt_modificacao, NOW()),
    CASE
        WHEN LOWER(TRIM(s.ativo)) IN ('s', 'sim', 'true') THEN 'S'
        ELSE 'N'
    END
FROM staging.ft_seguro_raw s
WHERE NOT EXISTS (
    SELECT 1 FROM dw.ft_seguro f
    WHERE f.chave_natural = (
        TRIM(s.nm_apolice) || '|' ||
        TO_CHAR(s.dt_inclusao, 'YYYY-MM-DD') || '|' ||
        TRIM(s.cd_filial) || '|' ||
        TO_CHAR(s.dt_contratacao, 'YYYY-MM-DD')
    )
);

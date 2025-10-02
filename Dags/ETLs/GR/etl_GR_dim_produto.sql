INSERT INTO dw.dim_produto (
    cd_produto, nm_produto, categoria, descricao, versao,
    dt_inicio, dt_fim, dt_modificacao
)
SELECT
    TRIM(s.cd_produto),
    TRIM(s.nm_produto),
    TRIM(s.categoria),
    TRIM(s.descricao),
    s.versao,
    s.dt_inicio,
    COALESCE(s.dt_fim, '9999-12-31 00:00:00'::timestamp) AS dt_fim,
    COALESCE(s.dt_modificacao, CURRENT_TIMESTAMP) AS dt_modificacao
FROM staging.dim_produto_raw s
WHERE NOT EXISTS (
    SELECT 1
    FROM dw.dim_produto d
    WHERE d.cd_produto = TRIM(s.cd_produto)
      AND d.versao = s.versao
);
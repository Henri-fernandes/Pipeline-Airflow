-- 1. Atualiza registros ativos que sofreram alteração
UPDATE dw.dim_filial d
SET ativo = 'N',
    dt_fim = CURRENT_DATE,
    dt_modificacao = CURRENT_TIMESTAMP
FROM staging.dim_filial_raw s
WHERE d.cd_filial = s.cd_filial
  AND d.ativo = 'S'
  AND (
      UPPER(TRIM(REPLACE(REPLACE(REPLACE(s.nm_filial, '  ', ' '), '.', ''), '  ', ' '))) <> d.nm_filial OR
      REGEXP_REPLACE(s.nr_cnpj, '[^0-9]', '', 'g') <> d.nr_cnpj OR
      UPPER(s.estado) <> d.estado OR
      UPPER(s.cidade) <> d.cidade
  );
-- 2. Insere nova versão apenas se não existir
INSERT INTO dw.dim_filial (
    nm_filial, nr_cnpj, cd_filial, chave_natural, estado, cidade,
    versao, ativo, dt_fundacao, dt_inicio, dt_fim, dt_modificacao
)
SELECT
    UPPER(TRIM(REPLACE(REPLACE(REPLACE(s.nm_filial, '  ', ' '), '.', ''), '  ', ' '))) AS nm_filial,
    REGEXP_REPLACE(s.nr_cnpj, '[^0-9]', '', 'g') AS nr_cnpj,
    s.cd_filial,
    s.cd_filial || '|' || REGEXP_REPLACE(s.nr_cnpj, '[^0-9]', '', 'g') AS chave_natural,
    UPPER(s.estado) AS estado,
    UPPER(s.cidade) AS cidade,
    s.versao,
    s.ativo,
    s.dt_fundacao,
    s.dt_inicio,
    COALESCE(s.dt_fim, DATE '9999-12-31'),
    COALESCE(s.dt_modificacao, CURRENT_TIMESTAMP)
FROM staging.dim_filial_raw s
WHERE NOT EXISTS (
    SELECT 1
    FROM dw.dim_filial d
    WHERE d.cd_filial = s.cd_filial
      AND d.nr_cnpj = REGEXP_REPLACE(s.nr_cnpj, '[^0-9]', '', 'g')
      AND d.versao = s.versao
);

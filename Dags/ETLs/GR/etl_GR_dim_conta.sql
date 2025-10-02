-- ETL GERAÇÃO DIM_CONTA - APENAS PESSOAS ATIVAS
-- Inserir dados novos na dimensão conta com campos tratados
-- Verificação adicional para garantir que apenas contas de pessoas ativas sejam inseridas

INSERT INTO dw.dim_conta (
    cd_conta, chave_natural, cpf_titular, tipo_conta, situacao, versao,
    dt_abertura, dt_inicio, dt_fim, dt_modificacao, cd_pessoa, cd_filial
)
SELECT
    s.cd_conta,
    s.cd_conta || '|' || UPPER(TRIM(s.tipo_conta)) || '|' || s.cd_filial AS chave_natural,
    REGEXP_REPLACE(s.cpf_titular, '[^0-9]', '', 'g') AS cpf_titular,
    UPPER(TRIM(s.tipo_conta)),
    UPPER(TRIM(s.situacao)),
    s.versao::INTEGER,
    s.dt_abertura,
    s.dt_inicio,
    COALESCE(s.dt_fim, DATE '9999-12-31'),
    COALESCE(s.dt_modificacao, CURRENT_TIMESTAMP),
    s.cd_pessoa,
    s.cd_filial
FROM staging.dim_conta_raw s
JOIN (
    -- Garantir que apenas pessoas ATIVAS na última versão sejam consideradas
    SELECT DISTINCT ON (cd_pessoa) cd_pessoa
    FROM dw.dim_pessoa 
    WHERE ativo = 'S'
    ORDER BY cd_pessoa, versao DESC
) p ON s.cd_pessoa = p.cd_pessoa
WHERE NOT EXISTS (
    SELECT 1
    FROM dw.dim_conta d
    WHERE d.cd_conta = s.cd_conta
      AND d.versao = s.versao::INTEGER
);
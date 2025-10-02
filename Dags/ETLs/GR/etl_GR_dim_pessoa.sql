-- 1. Inserir dados novos na dimensão pessoa com campos textuais em maiúsculo
INSERT INTO dw.dim_pessoa (
    nome_pessoa, cd_pessoa, nr_cpf, chave_natural, email, contato, salario,
    cidade, estado, sigla_estado, cd_filial, versao, ativo,
    dt_cadastro, dt_inicio, dt_fim, dt_modificacao
)
SELECT
    UPPER(TRIM(REPLACE(REPLACE(REPLACE(p.nome_pessoa, '  ', ' '), '.', ''), '  ', ' '))) AS nome_pessoa,
    p.cd_pessoa,
    REGEXP_REPLACE(p.nr_cpf, '[^0-9]', '', 'g') AS nr_cpf,
    p.cd_pessoa || '|' || REGEXP_REPLACE(p.nr_cpf, '[^0-9]', '', 'g') AS chave_natural,
    TRIM(p.email),
    REGEXP_REPLACE(p.contato, '[^0-9]', '', 'g') AS contato,  -- Remove tudo que não é número
    p.salario::NUMERIC,
    UPPER(p.cidade),
    UPPER(p.estado),
    UPPER(p.sigla_estado),
    p.cd_filial,
    p.versao,
    UPPER(p.ativo),
    p.dt_cadastro,
    p.dt_inicio,
    COALESCE(p.dt_fim, DATE '9999-12-31'),
    COALESCE(p.dt_modificacao, CURRENT_TIMESTAMP)
FROM staging.dim_pessoa_raw p
WHERE NOT EXISTS ( 
    SELECT 1
    FROM dw.dim_pessoa d
    WHERE d.cd_pessoa = p.cd_pessoa
      AND d.nr_cpf = REGEXP_REPLACE(p.nr_cpf, '[^0-9]', '', 'g')
      AND d.versao = p.versao
);
INSERT INTO dw.dim_pessoa (
    nome_pessoa,         -- Nome da pessoa, tratado e convertido para maiúsculo
    cd_pessoa,           -- Código único da pessoa
    nr_cpf,              -- CPF limpo (apenas números)
    chave_natural,       -- Chave composta: cd_pessoa + CPF
    email,               -- Email com espaços removidos
    contato,             -- Telefone limpo (apenas números)
    salario,             -- Valor do salário convertido para tipo numérico
    cidade,              -- Cidade em maiúsculo
    estado,              -- Estado em maiúsculo
    sigla_estado,        -- Sigla do estado em maiúsculo
    cd_filial,           -- Código da filial associada
    versao,              -- Versão do registro (controle histórico)
    ativo,               -- Indicador de ativo ('S' ou 'N'), convertido para maiúsculo
    dt_cadastro,         -- Data de cadastro original
    dt_inicio,           -- Data de início da vigência
    dt_fim,              -- Data de fim da vigência (default: 9999-12-31 se nula)
    dt_modificacao       -- Data da última modificação (default: CURRENT_TIMESTAMP se nula)
)
SELECT
    UPPER(TRIM(REPLACE(REPLACE(REPLACE(p.nome_pessoa, '  ', ' '), '.', ''), '  ', ' '))) AS nome_pessoa, -- Remove espaços duplicados,pontos e converte para maiúsculo
    p.cd_pessoa,                                        -- Mantém código original
    REGEXP_REPLACE(p.nr_cpf, '[^0-9]', '', 'g') AS nr_cpf,-- Remove tudo que não é número do CPF
    p.cd_pessoa || '|' || REGEXP_REPLACE(p.nr_cpf, '[^0-9]', '', 'g') AS chave_natural, -- Gera chave natural composta
    TRIM(p.email),                                      -- Remove espaços do email
    REGEXP_REPLACE(p.contato, '[^0-9]', '', 'g') AS contato, -- Remove tudo que não é número do telefone
    p.salario::NUMERIC,                                 -- Converte salário para tipo numérico
    UPPER(p.cidade),                                    -- Converte cidade para maiúsculo
    UPPER(p.estado),                                    -- Converte estado para maiúsculo
    UPPER(p.sigla_estado),                                                 -- Converte sigla do estado para maiúsculo
    p.cd_filial,                                        -- Mantém código da filial
    p.versao,                                            -- Mantém versão original
    UPPER(p.ativo),                                     -- Converte indicador de ativo para maiúsculo
    p.dt_cadastro,                                      -- Mantém data de cadastro original
    p.dt_inicio,                                        -- Mantém data de início original
    COALESCE(p.dt_fim, DATE '9999-12-31'),              -- Preenche fim com data padrão se nula
    COALESCE(p.dt_modificacao, CURRENT_TIMESTAMP)       -- Preenche modificação com timestamp atual se nula
FROM staging.dim_pessoa_raw p
-- Filtro para evitar duplicidade: só insere se código + CPF + versão ainda não existem
WHERE NOT EXISTS ( 
    SELECT 1
    FROM dw.dim_pessoa d
    WHERE d.cd_pessoa = p.cd_pessoa
      AND d.nr_cpf = REGEXP_REPLACE(p.nr_cpf, '[^0-9]', '', 'g')  -- Compara CPF limpo
      AND d.versao = p.versao                                     -- Compara versão do registro
)

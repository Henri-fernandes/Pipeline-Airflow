INSERT INTO dw.dim_conta (
    cd_conta,           -- Código único da conta
    chave_natural,      -- Chave composta: cd_conta + tipo_conta + cd_filial
    cpf_titular,        -- CPF do titular da conta (apenas números)
    tipo_conta,         -- Tipo da conta (ex: CRÉDITO, POUPANÇA)
    situacao,           -- Situação da conta (ex: ATIVA, BLOQUEADA)
    versao,             -- Versão do registro (controle histórico)
    dt_abertura,        -- Data de abertura da conta
    dt_inicio,          -- Data de início da vigência
    dt_fim,             -- Data de fim da vigência (default: 9999-12-31 se nula)
    dt_modificacao,     -- Data da última modificação (default: CURRENT_TIMESTAMP se nula)
    cd_pessoa,          -- Código da pessoa titular
    cd_filial           -- Código da filial onde a conta foi aberta
)
SELECT
    s.cd_conta,                                                                 -- Mantém código original da conta
    s.cd_conta || '|' || UPPER(TRIM(s.tipo_conta)) || '|' || s.cd_filial AS chave_natural, -- Gera chave natural composta
    REGEXP_REPLACE(s.cpf_titular, '[^0-9]', '', 'g') AS cpf_titular,           -- Remove tudo que não é número do CPF
    UPPER(TRIM(s.tipo_conta)),                                      -- Converte tipo da conta para maiúsculo e remove espaços
    UPPER(TRIM(s.situacao)),                                        -- Converte situação para maiúsculo e remove espaços
    s.versao::INTEGER,                                                         -- Converte versão para inteiro
    s.dt_abertura,                                                             -- Mantém data de abertura original
    s.dt_inicio,                                                               -- Mantém data de início original
    COALESCE(s.dt_fim, DATE '9999-12-31'),                                     -- Preenche fim com data padrão se nula
    COALESCE(s.dt_modificacao, CURRENT_TIMESTAMP),                         -- Preenche modificação com timestamp atual se nula
    s.cd_pessoa,                                                           -- Mantém código da pessoa
    s.cd_filial                                                            -- Mantém código da filial
FROM staging.dim_conta_raw s
-- Garante que apenas pessoas ativas na última versão sejam consideradas
JOIN (
    SELECT DISTINCT ON (cd_pessoa) cd_pessoa             -- Seleciona a versão mais recente de cada pessoa
    FROM dw.dim_pessoa 
    WHERE ativo = 'S'                                    -- Apenas pessoas ativas
    ORDER BY cd_pessoa, versao DESC                      -- Ordena por versão decrescente
) p ON s.cd_pessoa = p.cd_pessoa                         -- Faz o join com a pessoa ativa
WHERE NOT EXISTS (
    SELECT 1
    FROM dw.dim_conta d
    WHERE d.cd_conta = s.cd_conta                        -- Compara código da conta
      AND d.versao = s.versao::INTEGER                   -- Compara versão do registro
);

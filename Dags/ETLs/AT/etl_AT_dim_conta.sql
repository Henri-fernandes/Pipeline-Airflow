WITH pessoas_inativas AS ( -- Seleciona a versão mais recente de cada pessoa e Filtra apenas as pessoas inativas
    SELECT DISTINCT ON (cd_pessoa) 
        cd_pessoa, ativo
    FROM dw.dim_pessoa 
    ORDER BY cd_pessoa, versao DESC
),
contas_para_encerrar AS ( -- Seleciona a versão mais recente da conta e Filtra apenas as contas ativas que pertencem a pessoas inativas
    SELECT DISTINCT ON (c.cd_conta) 
        c.cd_conta, c.cd_pessoa, c.cd_filial, c.tipo_conta, c.situacao, 
        c.versao, c.dt_abertura, c.cpf_titular
    FROM dw.dim_conta c
    JOIN pessoas_inativas p ON c.cd_pessoa = p.cd_pessoa
    WHERE p.ativo = 'N'                             -- Pessoa está inativa
      AND UPPER(c.situacao) != 'ENCERRADA'          -- Conta ainda está ativa
    ORDER BY c.cd_conta, c.versao DESC
)
-- Insere uma nova versão da conta com situação ENCERRADA
INSERT INTO dw.dim_conta (
    cd_conta, chave_natural, cpf_titular, tipo_conta, situacao, versao,
    dt_abertura, dt_inicio, dt_fim, dt_modificacao, cd_pessoa, cd_filial
)
SELECT
    c.cd_conta,                                                           -- Mantém o código da conta
    c.cd_conta || '|' || UPPER(TRIM(c.tipo_conta)) || '|' || c.cd_filial, -- Gera chave natural única
    REGEXP_REPLACE(c.cpf_titular, '[^0-9]', '', 'g'),                     -- Limpa o CPF (só números)
    UPPER(TRIM(c.tipo_conta)),                                            -- Padroniza tipo da conta
    'ENCERRADA',                                                          -- Situação atualizada
    c.versao + 1,                                                         -- Incrementa a versão
    c.dt_abertura,                                                        -- Mantém data original de abertura
    CURRENT_DATE,                                                         -- Início da nova versão = hoje
    DATE '9999-12-31',                                                    -- Data de fim padrão
    CURRENT_TIMESTAMP,                                                    -- Data de modificação atual
    c.cd_pessoa,                                                          -- Código da pessoa
    c.cd_filial                                                           -- Código da filial
FROM contas_para_encerrar c;

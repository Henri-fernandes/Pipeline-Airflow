-- 1. Atualiza registros ativos que sofreram alteração
UPDATE dw.dim_filial d
SET ativo = 'N',                          -- Marca o registro antigo como inativo
    dt_fim = CURRENT_DATE,               -- Define a data de fim como hoje
    dt_modificacao = CURRENT_TIMESTAMP   -- Atualiza a data de modificação para agora
FROM staging.dim_filial_raw s
WHERE d.cd_filial = s.cd_filial          -- Compara pelo código da filial
  AND d.ativo = 'S'                      -- Só atualiza se o registro estiver ativo
  AND (                                  -- Verifica se houve alguma alteração relevante
      UPPER(TRIM(REPLACE(REPLACE(REPLACE(s.nm_filial, '  ', ' '), '.', ''), '  ', ' '))) <> d.nm_filial OR  -- Nome da filial mudou (limpo e em maiúsculo)
      REGEXP_REPLACE(s.nr_cnpj, '[^0-9]', '', 'g') <> d.nr_cnpj OR                       -- CNPJ mudou (apenas números)
      UPPER(s.estado) <> d.estado OR                                                                       -- Estado mudou
      UPPER(s.cidade) <> d.cidade                                                                      -- Cidade mudou
  );
-- 2. Insere nova versão apenas se não existir
INSERT INTO dw.dim_filial (
    nm_filial,            -- Nome da filial (limpo e em maiúsculo)
    nr_cnpj,              -- CNPJ limpo (apenas números)
    cd_filial,            -- Código da filial
    chave_natural,        -- Chave composta: cd_filial + CNPJ
    estado,               -- Estado em maiúsculo
    cidade,               -- Cidade em maiúsculo
    versao,               -- Versão do registro
    ativo,                -- Indicador de ativo ('S' ou 'N')
    dt_fundacao,          -- Data de fundação da filial
    dt_inicio,            -- Data de início da vigência
    dt_fim,               -- Data de fim da vigência (default: 9999-12-31 se nula)
    dt_modificacao        -- Data da última modificação (default: CURRENT_TIMESTAMP se nula)
)
SELECT
    UPPER(TRIM(REPLACE(REPLACE(REPLACE(s.nm_filial, '  ', ' '), '.', ''), '  ', ' '))) AS nm_filial, -- Limpa e padroniza o nome
    REGEXP_REPLACE(s.nr_cnpj, '[^0-9]', '', 'g') AS nr_cnpj,             -- Remove tudo que não é número do CNPJ
    s.cd_filial,                                                          -- Mantém código da filial
    s.cd_filial || '|' || REGEXP_REPLACE(s.nr_cnpj, '[^0-9]', '', 'g') AS chave_natural,         -- Gera chave natural composta
    UPPER(s.estado) AS estado,                                            -- Converte estado para maiúsculo
    UPPER(s.cidade) AS cidade,                                            -- Converte cidade para maiúsculo
    s.versao,                                                             -- Mantém versão original
    s.ativo,                                                              -- Mantém indicador de ativo
    s.dt_fundacao,                                                        -- Mantém data de fundação
    s.dt_inicio,                                                          -- Mantém data de início
    COALESCE(s.dt_fim, DATE '9999-12-31'),                                -- Preenche fim com data padrão se nula
    COALESCE(s.dt_modificacao, CURRENT_TIMESTAMP)                         -- Preenche modificação com timestamp atual se nula
FROM staging.dim_filial_raw s
WHERE NOT EXISTS (
    SELECT 1
    FROM dw.dim_filial d
    WHERE d.cd_filial = s.cd_filial                                       -- Compara código da filial
      AND d.nr_cnpj = REGEXP_REPLACE(s.nr_cnpj, '[^0-9]', '', 'g')        -- Compara CNPJ limpo
      AND d.versao = s.versao                                             -- Compara versão do registro
);

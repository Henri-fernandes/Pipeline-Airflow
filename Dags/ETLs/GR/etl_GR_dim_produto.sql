INSERT INTO dw.dim_produto (
    cd_produto,         -- Código único do produto
    nm_produto,         -- Nome do produto
    categoria,          -- Categoria do produto (ex: Conta, Crédito, Investimento)
    descricao,          -- Descrição detalhada do produto
    versao,             -- Versão do registro (para controle histórico)
    dt_inicio,          -- Data de início da vigência do registro
    dt_fim,             -- Data de fim da vigência (default: 9999-12-31 se nula)
    dt_modificacao      -- Data da última modificação (default: CURRENT_TIMESTAMP se nula)
)
SELECT
    TRIM(s.cd_produto),                                     -- Remove espaços do código do produto
    TRIM(s.nm_produto),                                     -- Remove espaços do nome do produto
    TRIM(s.categoria),                                      -- Remove espaços da categoria
    TRIM(s.descricao),                                      -- Remove espaços da descrição
    s.versao,                                                -- Mantém versão original
    s.dt_inicio,                                             -- Mantém data de início original
    COALESCE(s.dt_fim, '9999-12-31 00:00:00'::timestamp),    -- Preenche fim com data padrão se nula
    COALESCE(s.dt_modificacao, CURRENT_TIMESTAMP)            -- Preenche modificação com timestamp atual se nula
FROM staging.dim_produto_raw s
-- Filtro para evitar duplicidade: só insere se código + versão ainda não existem
WHERE NOT EXISTS (
    SELECT 1
    FROM dw.dim_produto d
    WHERE d.cd_produto = TRIM(s.cd_produto)     -- Compara código do produto
      AND d.versao = s.versao                   -- Compara versão do registro
);

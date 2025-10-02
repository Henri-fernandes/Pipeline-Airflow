-- 1. Identificar versão ativa de cada filial
WITH versoes_ativas AS (
    SELECT *
    FROM dw.dim_filial
    WHERE ativo = 'S'
),
-- 2. Buscar eventos mais recentes por campo
eventos AS (
    SELECT DISTINCT ON (cd_filial, campo_alterado)
           cd_filial, campo_alterado, valor_novo, data_evento
    FROM evt.ev_dim_filial
    WHERE tipo_evento = 'ALTERACAO'
    ORDER BY cd_filial, campo_alterado, data_evento DESC
),
-- 3. Juntar eventos com versão ativa
eventos_por_campo AS (
    SELECT
        va.cd_filial,
        va.nm_filial,
        va.nr_cnpj,
        va.estado,
        va.cidade,
        va.versao,
        va.dt_fundacao,
        ev_nome.valor_novo AS novo_nome,
        ev_estado.valor_novo AS novo_estado,
        ev_cidade.valor_novo AS nova_cidade,
        GREATEST(
            COALESCE(ev_nome.data_evento, DATE '1900-01-01'),
            COALESCE(ev_estado.data_evento, DATE '1900-01-01'),
            COALESCE(ev_cidade.data_evento, DATE '1900-01-01')
        ) AS maior_data_evento
    FROM versoes_ativas va
    LEFT JOIN eventos ev_nome ON va.cd_filial = ev_nome.cd_filial AND ev_nome.campo_alterado = 'nm_filial'
    LEFT JOIN eventos ev_estado ON va.cd_filial = ev_estado.cd_filial AND ev_estado.campo_alterado = 'estado'
    LEFT JOIN eventos ev_cidade ON va.cd_filial = ev_cidade.cd_filial AND ev_cidade.campo_alterado = 'cidade'
),
-- 4. Filiais que precisam ser atualizadas
filiais_a_atualizar AS (
    SELECT *
    FROM eventos_por_campo
    WHERE
        COALESCE(novo_nome, nm_filial) <> nm_filial OR
        COALESCE(novo_estado, estado) <> estado OR
        COALESCE(nova_cidade, cidade) <> cidade
),
-- 5. Encerrar versão atual
encerrar AS (
    UPDATE dw.dim_filial d
    SET ativo = 'N',
        dt_fim = CURRENT_DATE,
        dt_modificacao = CURRENT_TIMESTAMP
    FROM filiais_a_atualizar f
    WHERE d.cd_filial = f.cd_filial AND d.ativo = 'S'
    RETURNING d.*
)
-- 6. Inserir nova versão
INSERT INTO dw.dim_filial (
    cd_filial, nm_filial, nr_cnpj, estado, cidade,
    versao, ativo, dt_fundacao, dt_inicio, dt_fim,
    dt_modificacao, chave_natural
)
SELECT
    f.cd_filial,
    COALESCE(f.novo_nome, f.nm_filial),
    f.nr_cnpj,
    COALESCE(f.novo_estado, f.estado),
    COALESCE(f.nova_cidade, f.cidade),
    f.versao + 1,
    'S',
    f.dt_fundacao,
    CURRENT_DATE,
    DATE '9999-12-31',
    CURRENT_TIMESTAMP,
    f.cd_filial || '|' || f.nr_cnpj
    --date_trunc('month', f.maior_data_evento)::date AS dt_referencia
FROM filiais_a_atualizar f;

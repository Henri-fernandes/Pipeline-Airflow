-- 1. Identificar versão ativa de cada pessoa
WITH versoes_ativas AS (
    SELECT *
    FROM dw.dim_pessoa
    WHERE ativo = 'S'
),
-- 2. Buscar eventos mais recentes por campo
eventos AS (
    SELECT DISTINCT ON (cd_pessoa, campo_alterado)
           cd_pessoa, campo_alterado, valor_novo, data_evento
    FROM evt.ev_dim_pessoa
    WHERE tipo_evento = 'ALTERACAO'
    ORDER BY cd_pessoa, campo_alterado, data_evento DESC
),
-- 3. Juntar eventos com versão ativa
eventos_por_campo AS (
    SELECT
        va.cd_pessoa,
        va.nome_pessoa,
        va.nr_cpf,
        va.email,
        va.contato,
        va.salario,
        va.cidade,
        va.estado,
        va.sigla_estado,
        va.cd_filial,
        va.versao,
        va.dt_cadastro,
        ev_nome.valor_novo AS novo_nome,
        ev_email.valor_novo AS novo_email,
        ev_contato.valor_novo AS novo_contato,
        ev_salario.valor_novo::NUMERIC AS novo_salario,  -- Converte para NUMERIC
        ev_cidade.valor_novo AS nova_cidade,
        ev_estado.valor_novo AS novo_estado,
        ev_sigla.valor_novo AS nova_sigla,
        ev_filial.valor_novo AS nova_filial,
        GREATEST(
            COALESCE(ev_nome.data_evento, DATE '1900-01-01'),
            COALESCE(ev_email.data_evento, DATE '1900-01-01'),
            COALESCE(ev_contato.data_evento, DATE '1900-01-01'),
            COALESCE(ev_salario.data_evento, DATE '1900-01-01'),
            COALESCE(ev_cidade.data_evento, DATE '1900-01-01'),
            COALESCE(ev_estado.data_evento, DATE '1900-01-01'),
            COALESCE(ev_sigla.data_evento, DATE '1900-01-01'),
            COALESCE(ev_filial.data_evento, DATE '1900-01-01')
        ) AS maior_data_evento
    FROM versoes_ativas va
    LEFT JOIN eventos ev_nome   ON va.cd_pessoa = ev_nome.cd_pessoa AND ev_nome.campo_alterado = 'nome_pessoa'
    LEFT JOIN eventos ev_email  ON va.cd_pessoa = ev_email.cd_pessoa AND ev_email.campo_alterado = 'email'
    LEFT JOIN eventos ev_contato ON va.cd_pessoa = ev_contato.cd_pessoa AND ev_contato.campo_alterado = 'contato'
    LEFT JOIN eventos ev_salario ON va.cd_pessoa = ev_salario.cd_pessoa AND ev_salario.campo_alterado = 'salario'
    LEFT JOIN eventos ev_cidade  ON va.cd_pessoa = ev_cidade.cd_pessoa AND ev_cidade.campo_alterado = 'cidade'
    LEFT JOIN eventos ev_estado  ON va.cd_pessoa = ev_estado.cd_pessoa AND ev_estado.campo_alterado = 'estado'
    LEFT JOIN eventos ev_sigla   ON va.cd_pessoa = ev_sigla.cd_pessoa AND ev_sigla.campo_alterado = 'sigla_estado'
    LEFT JOIN eventos ev_filial  ON va.cd_pessoa = ev_filial.cd_pessoa AND ev_filial.campo_alterado = 'cd_filial'
),
-- 4. Pessoas que precisam ser atualizadas
pessoas_a_atualizar AS (
    SELECT *
    FROM eventos_por_campo
    WHERE
        COALESCE(novo_nome, nome_pessoa) <> nome_pessoa OR
        COALESCE(novo_email, email) <> email OR
        COALESCE(novo_contato, contato) <> contato OR
        COALESCE(novo_salario, salario) <> salario OR  -- Ambos já são NUMERIC agora
        COALESCE(nova_cidade, cidade) <> cidade OR
        COALESCE(novo_estado, estado) <> estado OR
        COALESCE(nova_sigla, sigla_estado) <> sigla_estado OR
        COALESCE(nova_filial, cd_filial) <> cd_filial
),
-- 5. Encerrar versão atual
encerrar AS (
    UPDATE dw.dim_pessoa d
    SET ativo = 'N',
        dt_fim = CURRENT_DATE,
        dt_modificacao = CURRENT_TIMESTAMP
    FROM pessoas_a_atualizar p
    WHERE d.cd_pessoa = p.cd_pessoa AND d.ativo = 'S'
    RETURNING d.*
)
-- 6. Inserir nova versão
INSERT INTO dw.dim_pessoa (
    nome_pessoa, cd_pessoa, nr_cpf, chave_natural, email, contato, salario,
    cidade, estado, sigla_estado, cd_filial, versao, ativo,
    dt_cadastro, dt_inicio, dt_fim, dt_modificacao
)
SELECT
    UPPER(COALESCE(p.novo_nome, p.nome_pessoa)),
    p.cd_pessoa,
    p.nr_cpf,
    UPPER(p.cd_pessoa || '|' || p.nr_cpf),
    COALESCE(p.novo_email, p.email),
    UPPER(COALESCE(p.novo_contato, p.contato)),
    COALESCE(p.novo_salario, p.salario),  -- Já convertido acima
    UPPER(COALESCE(p.nova_cidade, p.cidade)),
    UPPER(COALESCE(p.novo_estado, p.estado)),
    UPPER(COALESCE(p.nova_sigla, p.sigla_estado)),
    UPPER(COALESCE(p.nova_filial, p.cd_filial)),
    p.versao + 1,
    'S',
    p.dt_cadastro,
    CURRENT_DATE,
    DATE '9999-12-31',
    CURRENT_TIMESTAMP
FROM pessoas_a_atualizar p;
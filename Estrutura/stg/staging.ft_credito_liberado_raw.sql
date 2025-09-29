CREATE TABLE staging.ft_credito_liberado_raw (
    nr_contrato TEXT,
    vl_credito TEXT,
    vl_juros TEXT,
    vl_parcela TEXT,
    qtd_parcelas TEXT,
    tipo_credito TEXT,
    cd_produto TEXT,
    cd_filial TEXT,
    cd_pessoa TEXT,
    cd_conta TEXT,
    dt_contratacao TIMESTAMP,
    dt_inicio TIMESTAMP,
    dt_fim TIMESTAMP,
    dt_inclusao TIMESTAMP,
    dt_modificacao TIMESTAMP,
    ativo TEXT
)
CREATE TABLE staging.ft_investimento_raw (
    chave_natural TEXT,
    nr_aplicacao TEXT,
    tipo_investimento TEXT,
    vl_aplicado TEXT,
    vl_rendimento TEXT,
    vl_resgate TEXT,
    cd_produto TEXT,
    cd_filial TEXT,
    cd_pessoa TEXT,
    cd_conta TEXT,
    dt_aplicacao TIMESTAMP,
    dt_resgate TIMESTAMP,
    dt_inclusao TIMESTAMP,
    dt_modificacao TIMESTAMP,
    ativo TEXT
);
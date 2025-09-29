CREATE TABLE staging.dim_conta_raw (
    cd_conta        TEXT,
    chave_natural   TEXT,
    cpf_titular     TEXT,
    tipo_conta      TEXT,
    situacao        TEXT,
    versao          INT,
    dt_abertura     TIMESTAMP,
    dt_inicio       TIMESTAMP,
    dt_fim          TIMESTAMP,
    dt_modificacao  TIMESTAMP,
    cd_pessoa       TEXT,
    cd_filial       TEXT
);
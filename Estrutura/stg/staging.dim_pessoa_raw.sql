CREATE TABLE staging.dim_pessoa_raw (
    nome_pessoa     TEXT,
    cd_pessoa       TEXT,
    nr_cpf          TEXT,
    chave_natural   TEXT,
    email           TEXT,
    contato         TEXT,
    salario         TEXT,
    estado          TEXT,
    cidade			TEXT,
    sigla_estado    TEXT,
    cd_filial       TEXT,
    versao          INT,
    ativo           TEXT,
    dt_cadastro     TIMESTAMP,
    dt_inicio       TIMESTAMP,
    dt_fim          TIMESTAMP,
    dt_modificacao  TIMESTAMP
);
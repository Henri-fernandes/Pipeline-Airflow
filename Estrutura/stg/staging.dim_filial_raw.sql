CREATE TABLE staging.dim_filial_raw (
    nm_filial       TEXT,
    nr_cnpj         TEXT,
    cd_filial       TEXT,
    chave_natural   TEXT,
    estado          TEXT,
    cidade          TEXT,
    versao          INT,
    ativo           TEXT,
    dt_fundacao     TIMESTAMP,
    dt_inicio       TIMESTAMP,
    dt_fim          TIMESTAMP,
    dt_modificacao  TIMESTAMP
);
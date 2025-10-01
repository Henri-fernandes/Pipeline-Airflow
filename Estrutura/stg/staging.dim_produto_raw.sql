CREATE TABLE staging.dim_produto_raw (
    cd_produto      TEXT,
    nm_produto      TEXT,
    categoria       TEXT,
    descricao       TEXT,
    versao          INT,
    dt_inicio       TIMESTAMP,
    dt_fim          TIMESTAMP,
    dt_modificacao  TIMESTAMP
);
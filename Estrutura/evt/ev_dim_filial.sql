CREATE TABLE evt.ev_dim_filial (
    id_evento        SERIAL PRIMARY KEY,
    cd_filial        VARCHAR(5),
    campo_alterado   CHAR(50),
    valor_novo       TEXT,
    valor_anterior   TEXT,
    tipo_evento      VARCHAR(20),
    data_evento      TIMESTAMP,
    origem           VARCHAR(50),
    dt_insercao      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
CREATE TABLE evt.ev_dim_pessoa (
    id_evento        SERIAL PRIMARY KEY,
    cd_pessoa        CHAR(7),
    campo_alterado   VARCHAR(50),
    valor_novo       TEXT,
    valor_anterior   TEXT,
    tipo_evento      VARCHAR(20),
    data_evento      TIMESTAMP,
    origem           VARCHAR(50),
    dt_insercao      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
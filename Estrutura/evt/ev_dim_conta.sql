CREATE TABLE evt.ev_dim_conta (
    id_evento        SERIAL PRIMARY KEY,
    cd_conta         CHAR(10),
    campo_alterado   VARCHAR(50),
    valor_novo       TEXT,
    valor_anterior   TEXT,
    tipo_evento      VARCHAR(20),
    data_evento      TIMESTAMP,
    origem           VARCHAR(50),
    dt_insercao      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
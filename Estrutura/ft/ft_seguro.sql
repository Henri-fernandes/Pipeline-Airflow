CREATE TABLE dw.ft_seguro (
    id_seguro SERIAL PRIMARY KEY,
    chave_natural VARCHAR(100) NOT NULL,
    nm_apolice VARCHAR(30) NOT NULL,
    vl_premio NUMERIC(10,2) NOT NULL,
    vl_premio_liquido NUMERIC(10,2) NOT NULL,
    vl_comissao NUMERIC(5,2) NOT NULL,
    vl_cobertura NUMERIC(10,2) NOT NULL,
    sinistro CHAR(1) NOT NULL,
    tipo_seguro VARCHAR(30) NOT null,
    cd_produto VARCHAR(8) NOT NULL,
    cd_filial VARCHAR(5) NOT NULL, 
    cd_pessoa VARCHAR(7) NOT NULL,
    ativo CHAR(1) DEFAULT 'S' CHECK (ativo IN ('S','N')),
    dt_contratacao TIMESTAMP NOT NULL,
    dt_inicio TIMESTAMP NOT NULL,
    dt_fim TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    dt_inclusao TIMESTAMP NOT NULL DEFAULT NOW(),
    dt_modificacao TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_datas CHECK (dt_fim >= dt_inicio),
    CONSTRAINT unq_ft_seguro_chave_natural UNIQUE (chave_natural)
);

CREATE INDEX idx_apolice_ft_seguro
	ON dw.ft_seguro (nm_apolice);

CREATE INDEX idx_premio_ft_seguro
	ON dw.ft_seguro (vl_premio);

CREATE INDEX idx_premio_liquido_ft_seguro
	ON dw.ft_seguro (vl_premio_liquido);

CREATE INDEX idx_cd_produto_ft_seguro 
	ON dw.ft_seguro (cd_produto);

CREATE INDEX idx_contratacao_ft_seguro 
	ON dw.ft_seguro (dt_contratacao);

CREATE INDEX idx_inclusao_ft_seguro 
	ON dw.ft_seguro (dt_inclusao);
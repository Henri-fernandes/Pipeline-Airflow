CREATE TABLE dw.ft_credito_liberado (
    id_credito SERIAL PRIMARY KEY,
    chave_natural VARCHAR(100) NOT NULL, 
    nr_contrato VARCHAR(20) NOT NULL,
    vl_credito NUMERIC(12,2) NOT NULL,
    vl_juros NUMERIC(10,2) NOT NULL,
    vl_parcela NUMERIC(10,2) NOT NULL,
    qtd_parcelas INT NOT NULL,
    tipo_credito varchar(50) NOT NULL, 
    cd_produto char(8) NOT NULL,
    cd_filial CHAR(5) NOT NULL,
    cd_pessoa CHAR(7) NOT NULL,
    cd_conta CHAR(10) NOT NULL,
    ativo CHAR(1) DEFAULT 'S' CHECK (ativo IN ('S','N')),
    dt_contratacao TIMESTAMP NOT NULL,
    dt_inicio TIMESTAMP NOT NULL,
    dt_fim TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    dt_inclusao TIMESTAMP NOT NULL DEFAULT NOW(),
    dt_modificacao TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_datas CHECK (dt_fim >= dt_inicio),
    CONSTRAINT unq_chave_natural_ft_credito_liberado UNIQUE (chave_natural)
)

create index idx_contrato_ft_cred_liberado
	on dw.ft_credito_liberado(nr_contrato)
	
create index idx_vl_credito_ft_cred_liberado
	on dw.ft_credito_liberado(vl_credito)
	
create index idx_qnt_parcela_ft_cred_liberado
	on dw.ft_credito_liberado(qtd_parcelas)
	
create index idx_vl_parcela_ft_cred_liberado
	on dw.ft_credito_liberado(vl_parcela)

create index idx_tipo_credito_ft_cred_liberado
	on dw.ft_credito_liberado(tipo_credito)
	
create index idx_nome on dw.dim_produto(nm_produto)
CREATE TABLE dw.ft_investimento (
    id_investimento SERIAL PRIMARY KEY,
    chave_natural VARCHAR(100) NOT NULL, 
    nr_aplicacao VARCHAR(20) NOT NULL,
    tipo_investimento VARCHAR(30) NOT NULL, 
    vl_aplicado NUMERIC(12,2) NOT NULL,
    vl_rendimento NUMERIC(10,2),
    vl_resgate NUMERIC(12,2),
    cd_produto char(8) not null,
    cd_filial CHAR(5) not null,
    cd_pessoa CHAR(7) not null,
    cd_conta CHAR(10) not null,
    ativo CHAR(1) DEFAULT 'S' CHECK (ativo IN ('S','N')),
    dt_aplicacao TIMESTAMP NOT NULL,
    dt_resgate TIMESTAMP,
    dt_inclusao TIMESTAMP NOT NULL DEFAULT NOW(),
    dt_modificacao TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_datas CHECK (dt_resgate IS NULL OR dt_resgate >= dt_aplicacao),
    CONSTRAINT unq_chave_natural_investimento UNIQUE (chave_natural)
)

create index idx_tipo_investimento_ft_investimento	
	on dw.ft_investimento(tipo_investimento)
	
create index idx_vl_aplicado_ft_investimento	
	on dw.ft_investimento(vl_aplicado)
	
create index idx_vl_resgate_ft_investimento	
	on dw.ft_investimento(vl_resgate)
	
create index idx_dt_aplicacao_ft_investimento	
	on dw.ft_investimento(dt_aplicacao)
	
create index idx_dt_resgate_ft_investimento	
	on	dw.ft_investimento(dt_resgate)

create index idx_dt_inclusao_ft_investimento	
	on dw.ft_investimento(dt_inclusao)
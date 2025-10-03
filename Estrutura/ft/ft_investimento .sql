CREATE TABLE dw.ft_investimento (
	id_investimento serial primary key,
    chave_natural varchar(100) not null, 
    nr_aplicacao varchar(20) not null,
    tipo_investimento varchar(30) not null, 
    vl_aplicado numeric(12,2) not null,
    vl_rendimento numeric(10,2),
    vl_resgate numeric(12,2),
    cd_produto char(8) not null,
    cd_filial char(5) not null,
    cd_pessoa char(7) not null,
    cd_conta char(10) not null,
    ativo char(1) default 's' check (ativo in ('s','n')),
    dt_aplicacao timestamp not null,
    dt_resgate timestamp,
    dt_inclusao timestamp not null default now(),
    dt_modificacao timestamp not null default now(),
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
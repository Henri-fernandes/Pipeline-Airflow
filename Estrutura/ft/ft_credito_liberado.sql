CREATE TABLE dw.ft_credito_liberado (
    id_credito serial primary key,
    chave_natural varchar(100) not null, 
    nr_contrato varchar(20) not null,
    vl_credito numeric(12,2) not null,
    vl_juros numeric(10,2) not null,
    vl_parcela numeric(10,2) not null,
    qtd_parcelas int not null,
    tipo_credito varchar(50) not null, 
    cd_produto char(8) not null,
    cd_filial char(5) not null,
    cd_pessoa char(7) not null,
    cd_conta char(10) not null,
    ativo char(1) default 's' check (ativo in ('s','n')),
    dt_contratacao timestamp not null,
    dt_inicio timestamp not null,
    dt_fim timestamp not null default '9999-12-31',
    dt_inclusao timestamp not null default now(),
    dt_modificacao timestamp not null default now(),
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
	
CREATE TABLE dw.ft_seguro (
    id_seguro serial primary key,
    chave_natural varchar(100) not null,
    nm_apolice varchar(30) not null,
    vl_premio numeric(10,2) not null,
    vl_premio_liquido numeric(10,2) not null,
    vl_comissao numeric(5,2) not null,
    vl_cobertura numeric(10,2) not null,
    sinistro char(1) not null check (sinistro in ('s','n')),
    tipo_seguro varchar(30) not null,
    cd_produto varchar(8) not null,
    cd_filial varchar(5) not null, 
    cd_pessoa varchar(7) not null,
    ativo char(1) default 's' check (ativo in ('s','n')),
    dt_contratacao timestamp not null,
    dt_inicio timestamp not null,
    dt_fim timestamp not null default '9999-12-31',
    dt_inclusao timestamp not null default now(),
    dt_modificacao timestamp not null default now(),
    CONSTRAINT chk_datas CHECK (dt_fim >= dt_inicio),
    CONSTRAINT unq_ft_seguro_chave_natural UNIQUE (chave_natural)
);

create index idx_apolice_ft_seguro
	on dw.ft_seguro (nm_apolice);

create index idx_premio_ft_seguro
	on dw.ft_seguro (vl_premio);

create index idx_premio_liquido_ft_seguro
	on dw.ft_seguro (vl_premio_liquido);

create index idx_cd_produto_ft_seguro 
	on dw.ft_seguro (cd_produto);

create index idx_contratacao_ft_seguro 
	on dw.ft_seguro (dt_contratacao);

create index idx_inclusao_ft_seguro 
	on dw.ft_seguro (dt_inclusao);
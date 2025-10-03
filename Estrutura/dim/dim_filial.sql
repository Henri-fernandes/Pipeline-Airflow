create table dw.dim_filial (
	id_filial serial primary key,
	nm_filial varchar(100) not null,
	nr_cnpj char(14) not null,
	cd_filial varchar(5) not null,
	chave_natural char(20) not null,
	estado char(2) not null,
	cidade varchar(50) not null,
	versao int not null,
	ativo CHAR(1) default 'S' CHECK (ativo IN ('S','N')),
	dt_fundacao timestamp not null,
	dt_inicio timestamp not null,
	dt_fim timestamp not null default '9999-12-31',
	dt_modificacao timestamp not null default NOW(), 
	CONSTRAINT chk_datas CHECK (dt_fim >= dt_inicio), 
	CONSTRAINT unq_cnpj_versao_cd_filial UNIQUE (nr_cnpj, versao, cd_filial) s de uma  com mesmo cnpj e versao
)

create index idx_dim_filial_estado 
	on dw.dim_filial (estado)

create index idx_dim_filial_cidade 
	on dw.dim_filial (cidade);

create index idx_dim_nm_filial 
	on dw.dim_filial (nm_filial)
create table dw.dim_filial (
	id_filial SERIAL primary key,
	nm_filial varchar(100) not null,
	nr_cnpj char(14) not null,
	cd_filial varchar(5) not null,
	chave_natural char(20) not null,
	estado char(2) not null,
	cidade varchar(50) not null,
	versao int not null,
	ativo CHAR(1) DEFAULT 'S' CHECK (ativo IN ('S','N')),
	dt_fundacao TIMESTAMP not null,
	dt_inicio TIMESTAMP not null,
	dt_fim TIMESTAMP not null default '9999-12-31',
	dt_modificacao TIMESTAMP NOT NULL DEFAULT NOW(), 
	constraint chk_datas check (dt_fim >= dt_inicio), 
	CONSTRAINT unq_cnpj_versao_cd_filial UNIQUE (nr_cnpj, versao, cd_filial) s de uma  com mesmo cnpj e versao
)

CREATE INDEX idx_dim_filial_estado 
	ON dw.dim_filial (estado)

CREATE INDEX idx_dim_filial_cidade 
	ON dw.dim_filial (cidade);

create index idx_dim_nm_filial 
	on dw.dim_filial (nm_filial)
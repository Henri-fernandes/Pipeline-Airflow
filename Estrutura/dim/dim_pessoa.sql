CREATE TABLE dw.dim_pessoa (
    id_pessoa SERIAL PRIMARY KEY,       
    nome_pessoa varchar (100) not null,
    cd_pessoa char (7) not null,
    nr_cpf char(11) not null,
    chave_natural char (19) not null,
    email varchar(100),
    contato varchar(20),
    salario numeric(10,2) not null,
    cidade varchar(50) not null,
    estado varchar(50) not null,        
    sigla_estado char(2) not null,
    cd_filial char (5) not null,
    versao int not null,                
    ativo CHAR(1) DEFAULT 'S' CHECK (ativo IN ('S','N')),
    dt_cadastro date not null,
    dt_inicio date not null,
    dt_fim date not null default '9999-12-31',
    dt_modificacao timestamp not null default now(),
    dt_referencia date not null,
    CONSTRAINT chk_datas CHECK (dt_fim >= dt_inicio),
    CONSTRAINT unq_nr_cpf_versao_cd_pessoa UNIQUE (nr_cpf, versao, cd_pessoa)
)

create index idx_dim_pessoa_cidade
	on dw.dim_pessoa (cidade)
	
create index idx_dim_pessoa_estado 
	on dw.dim_pessoa (estado)
	
create index idx_dim_pessoa_salario 
	on dw.dim_pessoa (salario)
	
create index idx_dim_cd_pessoa 
	on dw.dim_pessoa (cd_pessoa)
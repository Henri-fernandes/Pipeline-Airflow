CREATE TABLE dw.dim_conta (
    id_conta serial primary key,              
    cd_conta char(10) not null,  		  
    chave_natural varchar(100) not null, 		  
    cpf_titular char(11), 	  
    tipo_conta varchar(20) not null,          
    situacao varchar(20) not null,            
    versao int not null,                      
    ativo char(1) default 's' check (ativo in ('s','n')),
    dt_abertura timestamp not null,
    dt_inicio timestamp not null,
    dt_fim timestamp not null default '9999-12-31',
    dt_modificacao timestamp not null default now(),
    cd_pessoa char(7), 
    cd_filial char(5),
    CONSTRAINT chk_datas CHECK (data_fim >= data_inicio),
    CONSTRAINT unq_chave_natural_versao UNIQUE (chave_natural, versao)
    
) 


create index idx_cpf_titular_dim_conta
	on dw.dim_conta(cpf_titular)
	
create index idx_cd_conta_dim_conta
	on dw.dim_conta(cd_conta)
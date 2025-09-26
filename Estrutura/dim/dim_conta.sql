CREATE TABLE dw.dim_conta (
    id_conta SERIAL PRIMARY KEY,              
    cd_conta CHAR(10) NOT NULL,  		  
    chave_natural VARCHAR(100) not null, 		  
    cpf_titular char(11), 	  
    tipo_conta VARCHAR(20) NOT NULL,          
    situacao VARCHAR(20) NOT NULL,            
    versao INT NOT NULL,                      
    ativo CHAR(1) DEFAULT 'S' CHECK (ativo IN ('S','N')),
    data_abertura TIMESTAMP NOT NULL,
    data_inicio TIMESTAMP NOT NULL,
    data_fim TIMESTAMP NOT NULL DEFAULT '9999-12-31',
    data_modificacao TIMESTAMP NOT NULL DEFAULT NOW(),
    cd_pessoa CHAR(7), 
    cd_filial CHAR(5), 
    CONSTRAINT chk_datas CHECK (data_fim >= data_inicio),
    CONSTRAINT unq_chave_natural_versao UNIQUE (chave_natural, versao)
    
)


create index idx_cpf_titular_dim_conta
	on dw.dim_conta(cpf_titular)
	
create index idx_cd_conta_dim_conta
	on dw.dim_conta(cd_conta)
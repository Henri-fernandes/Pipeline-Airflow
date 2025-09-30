CREATE TABLE dw.dim_produto (
    id_produto SERIAL PRIMARY KEY,          
    nm_produto varchar(50) NOT NULL,
    cd_produto char(8) not null,            
    categoria varchar(30) NOT NULL,
    descricao varchar(250),
    versao int not null,                   
    dt_fim timestamp NOT NULL DEFAULT '9999-12-31',
    dt_inicio timestamp NOT NULL DEFAULT CURRENT_DATE,
    dt_modificacao timestamp NOT NULL DEFAULT NOW(),
    CONSTRAINT unq_cd_produto_versao UNIQUE (cd_produto, versao), 
    CONSTRAINT chk_datas CHECK (dt_fim >= dt_inicio) 
);

  
create index idx_cd_produto 
	on dw.dim_produto (cd_produto)
	
create index idx_nm_produto 
	on dw.dim_produto (nm_produto)
	
create index idx_produto_categoria 
	on dw.dim_produto (categoria)
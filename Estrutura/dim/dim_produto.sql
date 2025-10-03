CREATE TABLE dw.dim_produto (
    id_produto serial primary key,          
    nm_produto varchar(50) not null,
    cd_produto char(8) not null,            
    categoria varchar(30) not null,
    descricao varchar(250),
    versao int not null,                   
    dt_fim timestamp not null default '9999-12-31',
    dt_inicio timestamp not null default current_date,
    dt_modificacao timestamp not null default NOW(),
    CONSTRAINT unq_cd_produto_versao UNIQUE (cd_produto, versao), 
    CONSTRAINT chk_datas CHECK (dt_fim >= dt_inicio) 
);

  
create index idx_cd_produto 
	on dw.dim_produto (cd_produto)
	
create index idx_nm_produto 
	on dw.dim_produto (nm_produto)
	
create index idx_produto_categoria 
	on dw.dim_produto (categoria)
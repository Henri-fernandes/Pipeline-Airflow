CREATE TABLE staging.ft_seguro_raw (
    chave_natural TEXT,             
    nm_apolice TEXT,                 
    vl_premio TEXT,                         
    vl_premio_liquido TEXT,                 
    vl_comissao TEXT,                       
    tipo_seguro TEXT,
    vl_cobertura TEXT,                     
    sinistro TEXT,                          
    cd_produto TEXT,                        
    cd_filial TEXT,                       
    cd_pessoa TEXT,                         
    dt_contratacao TIMESTAMP,                    
    dt_inicio TIMESTAMP,                         
    dt_fim TIMESTAMP ,                         
    dt_inclusao TIMESTAMP,                       
    dt_modificacao TIMESTAMP,                   
    ativo TEXT                                        
);
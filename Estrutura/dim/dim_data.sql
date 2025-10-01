CREATE TABLE dw.dim_data (
    data_id VARCHAR(20) PRIMARY KEY,   
    data DATE NOT NULL,
    ano INT NOT NULL,
    semestre INT NOT NULL,
    trimestre INT NOT NULL,
    mes INT NOT NULL,
    nome_mes VARCHAR(20) NOT NULL,
    abrev_mes CHAR(3) NOT NULL,        
    dia_mes INT NOT NULL,
    num_dia_semana INT NOT NULL,    
    dia_semana VARCHAR(20) NOT NULL,
    abrev_dia_semana CHAR(3) NOT NULL, 
    final_semana CHAR(1) NOT NULL,  
    dia_util CHAR(1) NOT NULL,     
    ano_mes CHAR(7) NOT NULL,          
    semana_mes INT NOT NULL,           
    dias_uteis INT,
    ultimo_dia_util DATE
);

WITH calendario AS (
    SELECT 
        d::DATE AS data,
        CONCAT(
            SUBSTRING(EXTRACT(YEAR FROM d)::text FROM 1 FOR 2), '.',
            LPAD(SUBSTRING(EXTRACT(YEAR FROM d)::text FROM 3 FOR 2), 2, '0'),
            SUBSTRING(TO_CHAR(d, 'MM') FROM 1 FOR 1), '.',
            SUBSTRING(TO_CHAR(d, 'MM') FROM 2 FOR 1) || TO_CHAR(d, 'DD')
        ) AS data_id,
        EXTRACT(YEAR FROM d)::INT AS ano,
        CASE WHEN EXTRACT(MONTH FROM d) <= 6 THEN 1 ELSE 2 END AS semestre,
        CEIL(EXTRACT(MONTH FROM d)/3.0)::INT AS trimestre,
        EXTRACT(MONTH FROM d)::INT AS mes,
        UPPER(TO_CHAR(d, 'TMMonth')) AS nome_mes,
        CASE EXTRACT(MONTH FROM d)
            WHEN 1 THEN 'JAN' WHEN 2 THEN 'FEV' WHEN 3 THEN 'MAR'
            WHEN 4 THEN 'ABR' WHEN 5 THEN 'MAI' WHEN 6 THEN 'JUN'
            WHEN 7 THEN 'JUL' WHEN 8 THEN 'AGO' WHEN 9 THEN 'SET'
            WHEN 10 THEN 'OUT' WHEN 11 THEN 'NOV' WHEN 12 THEN 'DEZ'
        END AS abrev_mes,
        EXTRACT(DAY FROM d)::INT AS dia_mes,
        EXTRACT(ISODOW FROM d)::INT AS num_dia_semana,
        UPPER(TO_CHAR(d, 'TMDay')) AS dia_semana,
        CASE EXTRACT(ISODOW FROM d)
            WHEN 1 THEN 'SEG' WHEN 2 THEN 'TER' WHEN 3 THEN 'QUA'
            WHEN 4 THEN 'QUI' WHEN 5 THEN 'SEX'
            WHEN 6 THEN 'SAB' WHEN 7 THEN 'DOM'
        END AS abrev_dia_semana,
        CASE WHEN EXTRACT(ISODOW FROM d) IN (6,7) THEN 'S' ELSE 'N' END AS final_semana,
        CASE WHEN EXTRACT(ISODOW FROM d) BETWEEN 1 AND 5 THEN 'S' ELSE 'N' END AS dia_util,
        CAST(EXTRACT(YEAR FROM d) AS TEXT) || LPAD(CAST(EXTRACT(MONTH FROM d) AS TEXT), 2, '0') AS ano_mes,
        CEIL(EXTRACT(DAY FROM d)/7.0)::INT AS semana_mes
    FROM generate_series('2000-01-01'::date, '2030-12-31'::date, interval '1 day') d
)
INSERT INTO dw.dim_data (
    data_id, data, ano, semestre, trimestre, mes, nome_mes, abrev_mes,
    dia_mes, num_dia_semana, dia_semana, abrev_dia_semana,
    final_semana, dia_util, ano_mes, semana_mes,
    dias_uteis, ultimo_dia_util
)
SELECT
    c.data_id,
    c.data,
    c.ano,
    c.semestre,
    c.trimestre,
    c.mes,
    c.nome_mes,
    c.abrev_mes,
    c.dia_mes,
    c.num_dia_semana,
    c.dia_semana,
    c.abrev_dia_semana,
    c.final_semana,
    c.dia_util,
    c.ano_mes,
    c.semana_mes,
    (SELECT COUNT(*) 
     FROM calendario c2
     WHERE c2.ano = c.ano 
       AND c2.mes = c.mes
       AND c2.dia_util = 'S') AS dias_uteis,
    (SELECT MAX(c3.data)
     FROM calendario c3
     WHERE c3.ano = c.ano 
       AND c3.mes = c.mes
       AND c3.dia_util = 'S') AS ultimo_dia_util
FROM calendario c


create index idx_data_data
	on  dw.dim_data (data)
	
create index idx__data_ano 
	on dw.dim_data (ano)
	
create index idx_data_mes
	on dw.dim_data (mes)

create index idx_data_ano_mes
	on dw.dim_data (ano_mes)

select * from dw.dim_data d 


alter table dw.dim_data d 
alter column dia_semana 

UPDATE dw.dim_data
SET dia_semana = UPPER(dia_semana);


drop table dw.dim_data2 
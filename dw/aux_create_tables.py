"""aux_create_tables.py
"""

AUX_DWUPDATE = """
CREATE TABLE aux_dwupdate
(
  DWUPDATE_SK SERIAL PRIMARY KEY
, UPDATE TIMESTAMP
, HOSTNAME VARCHAR(30)
, ELAPSED_TIME FLOAT8
);
"""

STG_CAGED = """
CREATE TABLE stg_caged
(
  COMPETENCIAMOV INTEGER 
, COMPETENCIADEC INTEGER 
, REGIAO SMALLINT
, UF SMALLINT
, MUNICIPIO INTEGER
, SECAO VARCHAR(1)
, SUBCLASSE INTEGER
, SALDOMOVIMENTACAO SMALLINT
, CBO2002OCUPACAO INTEGER
, CATEGORIA SMALLINT
, GRAUDEINSTRUCAO SMALLINT
, IDADE SMALLINT
, HORASCONTRATUAIS NUMERIC(8,2)
, RACACOR SMALLINT
, SEXO SMALLINT
, TIPOEMPREGADOR SMALLINT
, TIPOESTABELECIMENTO SMALLINT
, TIPOMOVIMENTACAO SMALLINT
, TIPODEDEFICIENCIA SMALLINT
, INDTRABINTERMITENTE SMALLINT
, INDTRABPARCIAL SMALLINT
, SALARIO NUMERIC(16,2)
, VALORSALARIOFIXO NUMERIC(16,2)
, TAMESTABJAN SMALLINT
, INDICADORAPRENDIZ SMALLINT --BOOLEAN
, ORIGEMDAINFORMACAO SMALLINT
, INDICADORDEFORADOPRAZO SMALLINT --BOOLEAN
, UNIDADESALARIOCODIGO SMALLINT
)
"""

DIM_DATE = """
CREATE TABLE dim_date
(
  DATE_SK SERIAL PRIMARY KEY
, YEAR_NUMBER SMALLINT
, YEARMO_NUMBER INTEGER
, MONTH_NUMBER SMALLINT
, DAY_OF_YEAR_NUMBER SMALLINT
, DAY_OF_MONTH_NUMBER SMALLINT
, DAY_OF_WEEK_NUMBER SMALLINT
, WEEK_OF_YEAR_NUMBER SMALLINT
, DAY_NAME VARCHAR(30)
, MONTH_NAME VARCHAR(30)
, MONTH_SHORT_NAME VARCHAR(3)
, QUARTER_NUMBER SMALLINT
, WEEKEND_IND BOOLEAN
, DAYS_IN_MONTH SMALLINT
, DAY_DESC TEXT
, DAY_DATE TIMESTAMP
, WEEK_OF_MONTH_NUMBER SMALLINT
, YEAR_HALF_NUMBER SMALLINT
, LEAP_YEAR BOOLEAN
)
;CREATE INDEX idx_dim_date_lookup ON dim_date(DATE_SK)
;
"""

DIM_MUNICIPIO = """
CREATE TABLE dim_municipio
(
  MUNICIPIO_SK SERIAL PRIMARY KEY
, COD_SIAFI INT
, COD_IBGE INT
, CNPJ VARCHAR(14)
, UF VARCHAR(2)
, NOME VARCHAR(60)
);
"""

# Data Warehouse Structure
DW_TABLES = dict(
    stg_caged=STG_CAGED,
    dim_date=DIM_DATE,
    dim_municipio=DIM_MUNICIPIO,
    aux_dwupdate=AUX_DWUPDATE,
)

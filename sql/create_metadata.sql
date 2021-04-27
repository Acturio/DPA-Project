-- Creation of schema metadata
DROP SCHEMA IF EXISTS metadata CASCADE;
CREATE SCHEMA metadata;

-- Creation of table metadata.ingestion
DROP TABLE IF EXISTS metadata.ingestion;
CREATE TABLE metadata.ingestion
(
	file_name varchar,
	data_date date,
	processing_date TIMESTAMPTZ,
	nrows INTEGER,
	ncols INTEGER,
	extension_file varchar,
	col_names varchar,
	source varchar,
	dataset varchar	
);

-- Creation of table metadata.almacenamiento
DROP TABLE IF EXISTS metadata.almacenamiento;
CREATE TABLE metadata.almacenamiento
(
        file_name varchar,
        data_date date,
        processing_date TIMESTAMPTZ,
        nrows INTEGER,
        ncols INTEGER,
        extension_file varchar,
        col_names varchar,
        source varchar,
        dataset varchar 
);

-- Creation of table metadata.cleaning
DROP TABLE IF EXISTS metadata.cleaning;
CREATE TABLE metadata.cleaning
(
        file_name varchar,
        data_date date,
        processing_date TIMESTAMPTZ,
        nrows INTEGER,
        ncols INTEGER,
        extension_file varchar,
        col_names varchar,
        source varchar,
        dataset varchar 
);

-- Creation of table metadata.feature
DROP TABLE IF EXISTS metadata.feature;
CREATE TABLE metadata.feature
(
        file_name varchar,
        data_date date,
        processing_date TIMESTAMPTZ,
        nrows INTEGER,
        ncols INTEGER,
        extension_file varchar,
        col_names varchar,
        source varchar,
        dataset varchar 
);

-- FALTA TABLA DE ENTRENAMIENTO Y SELECCIÓN -------------

-- Creation of table metadata.entrenamiento
DROP TABLE IF EXISTS metadata.entrenamiento;
CREATE TABLE metadata.entrenamiento
(
        processing_date TIMESTAMPTZ,
        data_date date,
	estimator varchar,
        scoring varchar,
        params varchar,
        mean_test_score decimal(10,6),
        rank_test_score integer
);

-- Creation of table metadata.seleccion
DROP TABLE IF EXISTS metadata.seleccion;
CREATE TABLE metadata.seleccion
(
        processing_date TIMESTAMPTZ,
        data_date date,
        value_params varchar 
);

-- Drop luigi table
DROP TABLE IF EXISTS public.table_updates;

-- METADATA TEST ---------------------------------------

-- Creation of table metadata.test_ingestion
DROP TABLE IF EXISTS metadata.test_ingestion;
CREATE TABLE metadata.test_ingestion
(
	file_name varchar,
	data_date date,
	processing_date TIMESTAMPTZ,
	test_name varchar,
	result boolean	
);

-- Creation of table metadata.test_almacenamiento
DROP TABLE IF EXISTS metadata.test_almacenamiento;
CREATE TABLE metadata.test_almacenamiento
(
        file_name varchar,
	data_date date,
	processing_date TIMESTAMPTZ,
	test_name varchar,
	result boolean
);

-- Creation of table metadata.test_cleaning
DROP TABLE IF EXISTS metadata.test_cleaning;
CREATE TABLE metadata.test_cleaning
(
        file_name varchar,
	data_date date,
	processing_date TIMESTAMPTZ,
	test_name varchar,
	result boolean 
);

-- Creation of table metadata.test_feature
DROP TABLE IF EXISTS metadata.test_feature;
CREATE TABLE metadata.test_feature
(
        file_name varchar,
	data_date date,
	processing_date TIMESTAMPTZ,
	test_name varchar,
	result boolean
);

-- Creation of table metadata.test_entrenamiento
DROP TABLE IF EXISTS metadata.test_entrenamiento;
CREATE TABLE metadata.test_entrenamiento
(
        file_name varchar,
	data_date date,
	processing_date TIMESTAMPTZ,
	test_name varchar,
	result boolean
);

-- Creation of table metadata.test_seleccion
DROP TABLE IF EXISTS metadata.test_seleccion;
CREATE TABLE metadata.test_seleccion
(
        file_name varchar,
	data_date date,
	processing_date TIMESTAMPTZ,
	test_name varchar,
	result boolean
);




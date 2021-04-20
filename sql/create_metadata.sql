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

-- Drop luigi table
DROP TABLE IF EXISTS public.table_updates;

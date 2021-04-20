-- creation of ingestion table
DROP SCHEMA IF EXISTS metadata CASCADE;

CREATE SCHEMA metadata;

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

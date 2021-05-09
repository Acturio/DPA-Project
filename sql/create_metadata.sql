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

-- Sesgo e inequidad
-- Creation of schema sesgo
DROP SCHEMA IF EXISTS sesgo CASCADE;
CREATE SCHEMA sesgo;

DROP TABLE IF EXISTS sesgo.bias_fairness;
CREATE TABLE sesgo.bias_fairness
(
	fecha_load DATE,
	fecha DATE,
	model_id INTEGER,
	score_threshold VARCHAR,
	k INTEGER,
	attribute_name VARCHAR,
	attribute_value VARCHAR,
	tpr DOUBLE,
	tnr DOUBLE,
	[for] DOUBLE,
	fdr DOUBLE,
	fpr DOUBLE,
	fnr DOUBLE,
	npv DOUBLE,
	precision DOUBLE,
	pp INTEGER,
	pn INTEGER,
	ppr DOUBLE,
	pprev DOUBLE,
	fp INTEGER,
	fn INTEGER,
	tn INTEGER,
	tp INTEGER,
	group_label_pos INTEGER,
	group_label_neg INTEGER,
	group_size INTEGER,
	total_entities INTEGER,
	prev DOUBLE, 
	ppr_disparity DOUBLE,
	pprev_disparity DOUBLE,
	precision_disparity DOUBLE,
	fdr_disparity DOUBLE,
	for_disparity DOUBLE,
	fpr_disparity DOUBLE,
	fnr_disparity DOUBLE,
	tpr_disparity DOUBLE,
	tnr_disparity DOUBLE,
	npv_disparity DOUBLE,
	ppr_ref_group_value VARCHAR,
	pprev_ref_group_value VARCHAR,
	precision_ref_group_value VARCHAR,
	fdr_ref_group_value VARCHAR,
	for_ref_group_value VARCHAR,
	fpr_ref_group_value VARCHAR,
	fnr_ref_group_value VARCHAR,
	tpr_ref_group_value VARCHAR,
	tnr_ref_group_value VARCHAR,
	npv_ref_group_value VARCHAR,
	[Statistical Parity] BOOLEAN,
	[Impact Parity] BOOLEAN,
	[FDR Parity] BOOLEAN,
	[FPR Parity] BOOLEAN,
	[FOR Parity] BOOLEAN,
	[FNR Parity] BOOLEAN,
	[TPR Parity] BOOLEAN,
	[TNR Parity] BOOLEAN,
	[NPV Parity] BOOLEAN,
	[Precision Parity] BOOLEAN,
	[TypeI Parity] BOOLEAN,
	[Equalized Odds] BOOLEAN,
	[Unsupervised Fairness] BOOLEAN,
	[Supervised Fairness] BOOLEAN
);







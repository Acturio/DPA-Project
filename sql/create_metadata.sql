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
	tpr decimal(20,10),
	tnr decimal(20,10),
	f_or decimal(20,10),
	fdr decimal(20,10),
	fpr decimal(20,10),
	fnr decimal(20,10),
	npv decimal(20,10),
	"precision" decimal(20,10),
	pp INTEGER,
	pn INTEGER,
	ppr decimal(20,10),
	pprev decimal(20,10),
	fp INTEGER,
	fn INTEGER,
	tn INTEGER,
	tp INTEGER,
	group_label_pos INTEGER,
	group_label_neg INTEGER,
	group_size INTEGER,
	total_entities INTEGER,
	prev decimal(20,10), 
	ppr_disparity decimal(20,10),
	pprev_disparity decimal(20,10),
	precision_disparity decimal(20,10),
	fdr_disparity decimal(20,10),
	for_disparity decimal(20,10),
	fpr_disparity decimal(20,10),
	fnr_disparity decimal(20,10),
	tpr_disparity decimal(20,10),
	tnr_disparity decimal(20,10),
	npv_disparity decimal(20,10),
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
	statistical_parity BOOLEAN,
	impact_parity BOOLEAN,
	fdr_parity BOOLEAN,
	fpr_parity BOOLEAN,
	for_parity BOOLEAN,
	fnr_parity BOOLEAN,
	tpr_parity BOOLEAN,
	tnr_parity BOOLEAN,
	npv_parity BOOLEAN,
	precision_parity BOOLEAN,
	typei_parity BOOLEAN,
	typeii_parity BOOLEAN,
	equalized_odds BOOLEAN,
	unsupervised_fairness BOOLEAN,
	supervised_fairness BOOLEAN
);







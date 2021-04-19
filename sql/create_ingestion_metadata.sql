-- creation of ingestion table
DROP TABLE IF EXISTS raw.ingestion;

CREATE TABLE raw.ingestion(
  inspection_id varchar DEFAULT NULL,
  dba_name varchar DEFAULT NULL,
  aka_name varchar DEFAULT NULL,
  license_ varchar DEFAULT NULL,
  facility_type varchar DEFAULT NULL,
  risk varchar DEFAULT NULL,
  address varchar DEFAULT NULL,
  city varchar DEFAULT NULL,
  state varchar DEFAULT NULL,
  inspection_date varchar DEFAULT NULL,
  inspection_type varchar DEFAULT NULL,
  results varchar DEFAULT NULL,
  latitude varchar DEFAULT NULL,
  longitude varchar DEFAULT NULL,
  location json DEFAULT NULL,
  violations varchar DEFAULT NULL
);

cat("\n ")
cat("¡¡AUXILIO!! Python necesita ayuda de R")
cat("\n ")
cat("¡R al rescate! Iniciando R!")
cat("\n ")
cat("Leyendo librerías")

library(dplyr)
library(RPostgres)
library(DBI)
library(yaml)
library(mlflow)
library(tidyr)
library(stringr)

setwd('/home/acturio/documents/DPA-Project')
Sys.setenv(MLFLOW_PYTHON_BIN="/home/acturio/.pyenv/versions/dpa-itam-3.7.4/bin/python")

cat("\n ")
cat("Abriendo proyecto de MLFlow")
mlflow_set_experiment(experiment_name = "dpa-experiment")

cat("\n ")
cat("Leyendo credenciales")
credentials <- read_yaml("./conf/local/credentials.yaml", fileEncoding = "UTF-8")
db_cred <- credentials$db

db <- db_cred$database  
host_db <- db_cred$host 
db_port <- db_cred$port
db_user <- db_cred$user  
db_password <- db_cred$pass

cat("\n ")
cat("Conectándose a la base en postgres")
con <- dbConnect(
  RPostgres::Postgres(), 
  dbname = db, 
  host = host_db, 
  port = db_port, 
  user = db_user, 
  password = db_password,
  options = " -c search_path=metadata"
  )

cat("\n ")
cat("Leyendo datos de psotgres: metadatos de modelos")
data <- tbl(con, "entrenamiento")

model_results <- data %>%
  tidyr::pivot_wider(names_from = parameter, values_from = value) %>%
  arrange(data_date, estimator, num_model) %>% collect()

cat("\n ")
cat("Creando logs de hiperparámetros y métricas")
for (i in 1:nrow(model_results)) {

  row <- model_results[i,]
   if(str_sub(row$estimator, 1, 1) == "D"){
     with(mlflow_start_run(), {
  
       mlflow_log_param("max_depth", row$max_depth)
       mlflow_log_param("min_samples_leaf", row$min_samples_leaf)
       mlflow_log_metric("recall", row$mean_test_score)  
       mlflow_set_tag("Model", "DecisionTreeClassifier")
       mlflow_set_tag("Version", row$num_model)
    })
   }else if(str_sub(row$estimator, 1, 1) == "L")
     with(mlflow_start_run(), {
  
       mlflow_log_param("penalty", row$penalty)
       mlflow_log_param("C", row$C)
       mlflow_log_metric("recall", row$mean_test_score)  
       mlflow_set_tag("Model", "LogisticRegression")
       mlflow_set_tag("Version", row$num_model)
    })else if(str_sub(row$estimator, 1, 1) == "R"){
     with(mlflow_start_run(), {
  
       mlflow_log_param("n_estimators", row$n_estimators)
       mlflow_log_param("max_depth", row$max_depth)
       mlflow_log_param("min_samples_leaf", row$min_samples_leaf)
       mlflow_log_metric("recall", row$mean_test_score)  
       mlflow_set_tag("Model", "RandomForestClassifier")
       mlflow_set_tag("Version", row$num_model)
    })
   }
}

cat("\n ")
cat("Proceso terminado")

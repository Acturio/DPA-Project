<<<<<<< HEAD
#library(dplyr)
#library(Rpostgres)
#library(DBI)
=======
>>>>>>> 185160fd7723de9ed54e7219bd6617bd8dccc55f
library(infuser)

args <- commandArgs(TRUE)
print(args)

parameter1 <- as.character(args[1])
parameter2 <- as.character(args[2])
parameter3 <- as.numeric(args[3])
parameter4 <- as.logical(args[4])
#print(parameter1)
#print(parameter2)

data <- data.frame(
  parameter = c("date", "exercise"), 
  value = c(parameter1, parameter2),
  numerics = c(parameter3, 5),
  bool = c(FALSE, parameter4)
  )
print(data)

write.csv(data, "sandbox/test_r2.csv", row.names = F)
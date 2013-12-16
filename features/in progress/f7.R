# Get feature: time to next click 
#   for each click is time before user do another click
#   for last click in session is -1

library("sqldf")

### Set connection with database ###
base = "~/workspace/kaggle/subset50k.train.sqlite"
db <-dbConnect(SQLite(), dbname=base)
dbListTables(db) 

extractFeature  <- function(tab_name, feature_name) {

  sqldf(paste(
    "DROP TABLE IF EXISTS ", tab_name, ";"), 
  connection=db) 
  
  sqldf(paste(
    "CREATE TABLE ", tab_name, " as ",
    "SELECT c1.id as click_id, c2.time_passed-c1.time_passed as ", feature_name,
    "FROM click c1",
      "LEFT JOIN click c2 ON",
        "c2.rowid = c1.rowid+1;"), 
  connection=db) 
  
  sqldf(paste(
    "UPDATE ", tab_name,
    "SET ", feature_name, "=-1",
    "WHERE ", feature_name, "<0 or ", feature_name , " IS NULL ;"), 
  connection=db)
  
}

###Choose clear table and feature name, please ###
extractFeature(tab_name = "f7", feature_name = "val")

dbDisconnect(db)

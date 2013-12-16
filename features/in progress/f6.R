# Get feature: entropy by the same query

library("sqldf")

### Set connection with database ###
base = "~/workspace/kaggle/subset50k.train.sqlite"
db <-dbConnect(SQLite(), dbname=base)
dbListTables(db)

extractFeature  <- function(tab_name, feature_name) {

  sqldf("
  DROP TABLE IF EXISTS tab_query_count;
  ", connection=db)
  
  sqldf(paste(
  "DROP TABLE IF EXISTS ", tab_name), 
  connection=db)
  
  sqldf(paste(
  "DROP TABLE IF EXISTS support"), 
  connection=db)
  
  sqldf("
  CREATE TABLE support AS SELECT user_id, query_id FROM
  session INNER JOIN query_action ON session.id = query_action.session_id
  ", dbname=base) 
  
  sqldf("
    CREATE TABLE tab_query_count AS
    SELECT t1.user_id, t1.query_id, query_count, user_query_count FROM 
      ((SELECT user_id, query_id, count(query_id) AS query_count
      FROM support
      GROUP BY user_id, query_id ORDER BY user_id) AS t1
    LEFT JOIN
      (SELECT user_id, count(query_id) AS user_query_count
      FROM support
      GROUP BY user_id ORDER BY user_id) AS t2
    on t1.user_id = t2.user_id);",
    connection=db)

  sqldf("
    ALTER TABLE tab_query_count
    ADD COLUMN frequency float;",
    connection=db)
  
  sqldf("
    UPDATE tab_query_count SET frequency = cast(query_count as float)/user_query_count;", 
    dbname=base)

  sqldf(paste("
    CREATE TABLE ", tab_name, 
    "AS SELECT user_id,
      -SUM(frequency*LOG(frequency)) as ", feature_name, 
    "FROM tab_query_count
    GROUP BY user_id;"), 
    dbname=base)
  
  sqldf("
  DROP TABLE IF EXISTS tab_query_count;
  ", dbname=base)
  
  sqldf("
  DROP TABLE IF EXISTS support;
  ", dbname=base)
}

###Choose clear table and feature name, please ###
extractFeature(tab_name = "f6", feature_name = "val")
dbDisconnect(db)

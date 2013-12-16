#!usr/bin/Rscript

###Extract the average of ratio between unique queries and all queries issued by user during all his/her sessions 
###To get population: f5.val 

###Some primary settings
#setwd("")
#system(“ls *.bd”)
#...
options(max.print=50)

###Load package
is.installed <- require(sqldf)

###Choose DB to write. Please, remember to run your script for smaller DB first
base <- "./dataset.db"

###Set connection
db <-dbConnect(SQLite(), dbname=base)
dbListTables(db) 

###DB writing
extractFeature  <- function(tb.name, f.name) {

  sqldf(sub("\\%",tb.name,"DROP TABLE IF EXISTS %"),connection=db)  
  
  sqldf(paste("CREATE TABLE ",tb.name, " as 
				SELECT user_id, 1-AVG(freq) as ", f.name,  
                                 " from 
                                  session S INNER JOIN (
                                  SELECT session_id,
                                        unique_queries/all_queries as freq 
                                   from (
                                        SELECT session_id,
                                               CAST(COUNT(DISTINCT query_id) as double) as unique_queries, 
                                               CAST(COUNT(QA.id) as double) as all_queries 
                                         from 
                                          session S LEFT JOIN query_action QA on S.id=QA.session_id
                                        GROUP BY session_id
                                   )
                         ) AS F on S.id=F.session_id GROUP BY user_id"),connection=db)  
  
  sqldf(sub("\\%",tb.name,"SELECT * from %"),connection=db)
} 

###Choose standard table and feature name, please
extractFeature(tb.name ="f5",f.name="val")

###Close connection
dbDisconnect(db)


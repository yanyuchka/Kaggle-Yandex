#!usr/bin/Rscript

###Extract the ratio between all clicks and sessions made by user
###To get population: f4.val 

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
				SELECT user_id as id, 
					CAST(COUNT(C.id) as double)/CAST(COUNT(S.id) as double)  as ", f.name, 
				" from 
				   session S LEFT JOIN click C on S.id=session_id
                                GROUP BY user_id"),connection=db)  
  
  sqldf(sub("\\%",tb.name,"SELECT * from %"),connection=db)
} 

###Choose standard table and feature name, please
extractFeature(tb.name ="f4",f.name="val")

###Close connection
dbDisconnect(db)


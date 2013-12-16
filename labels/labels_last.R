#!usr/bin/Rscript

###Scoring URLs' relevance 
###To get population: labels.val 

###Some primary settings
#...
setwd("C:/Kaggle/")
options(max.print=100)

###Load package
is.installed <- require(sqldf)

###Choose DB to write. Please, remember to run your script for smaller DB first
base.to.write <- ".\small.features.db"

###Set connection
db <-dbConnect(SQLite(), dbname=base.to.write)
dbListTables(db) 

###DB writing
extractFeature  <- function(tb.name, f.name) {
  
  sqldf(sub("\\%",tb.name,"DROP TABLE IF EXISTS %"),connection=db)  
  
  sqldf("DROP TABLE IF EXISTS temp_for_labels",connection=db)
  
  print("OK, I have started...")

  sqldf("CREATE TABLE temp_for_labels as
		SELECT C1.url_id as id, 
		C2.time_passed - C1.time_passed as dwell
          from click C1 LEFT JOIN click C2 on C2.rowid=C1.rowid+1",connection=db)
  
  print("dwell time is calculated")

  sqldf("UPDATE temp_for_labels SET dwell=401 
        WHERE rowid in (SELECT MAX(rowid) from click GROUP BY session_id)", connection=db)

  sqldf("UPDATE temp_for_labels SET dwell=401 WHERE rowid in 
  			(SELECT rowid from temp_for_labels GROUP BY id HAVING(COUNT(rowid)>=2))", connection=db)
 
  print("dwell time is updated")

  sqldf(paste("CREATE TABLE ", tb.name, "as
		SELECT id,
		CASE WHEN dwell>400 THEN 2
		     WHEN dwell>=50 AND dwell<=400 THEN 1
		     WHEN dwell>0 AND dwell<50 OR dwell is NULL THEN 0
		     WHEN dwell=0 THEN NULL
		END as ", f.name, " from url LEFT JOIN temp_for_labels USING(id)"), connection=db)

  print("labels are added!")
  
  print("first 50 records at labels tab")
  
  sqldf("DROP TABLE IF EXISTS temp_for_labels",connection=db)

  sqldf(sub("\\%", tb.name, "SELECT * from %"),connection=db)

} 

###Choose standard table and feature name, please
extractFeature(tb.name ="labels",f.name="val")

###Close connection
conn <- dbDisconnect(db)


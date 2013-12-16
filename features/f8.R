#!/usr/bin/Rscript

###URL simple feautures: URL's domain popularity ###
###To get population: f8.val

###Some settings
options(max.print=20)

###Load package
is.installed <- require(sqldf)

###Choose DB to write. Please, remember to run your script for smaller DB first
base.to.write <- "dataset.db"

###DB writing
extractFeature <- function(name,f.name) {
  sqldf(sub("\\%",f.name,sub("\\%", name, "CREATE TABLE % AS 
                                SELECT U.id,
				      CASE WHEN clicks is NULL THEN 0 ELSE clicks END AS % 
				 from url AS U LEFT JOIN (
				SELECT url_id, COUNT(id) AS clicks 
				 from click GROUP BY url_id) on U.id=url_id")),dbname=base.to.write)

  sqldf(sub("\\%", name, "SELECT * from %"),dbname=base.to.write)
}

###Choose standard feature and table name, please
extractFeature(name ="f8",f.name="val")



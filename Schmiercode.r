library(R6)
library(dplyr)

test <- R6Class(
  "DatabaseConnections",
  public = list(
    initialize = function(.name, .value)
    {
      self$set("active", "getx2", 4)
    }
  ),
  active = list(
    test2 = function() "hello world"
  ),
  private = list(
    a = 4
  )
)



connect_db <- function()
{
db <- dbconnection$new("statup")

DatabaseModel <- R6Class(
  "DatabaseModel",
  public = list(),
  active = list(),
  private = list()
)

for(table in src_tbls(statup$getdb))
  DatabaseModel$set("public", table, tbl(db$getdb, table))

DatabaseModel$new()
}


A <- connect_db()


tbl_active <- R6Class(
  public = list(
    initialize = function(.tblname)
    {
      private$p_dbmodel <- dbconnection$new("statup")
      private$p_tblname <- .tblname
      private$p_tbl     <- tbl(private$p_dbmodel$getdb, .tblname)
    }
  ),
  active = list(
    get     = function() private$p_tbl,
    set     = function(.value) {},
    update  = function(.value) 
    {
      DatabaseWriter(.result = value, 
                     .dbmodel = private$p_dbmodel,
                     .tblname = private$p_tblname,
                     .chunksize = 1e5, 
                     .n = NULL,
                     .ConstructQuery = ConstructUpdateQuery)
    },
    replace = function(.value) 
    {
      DatabaseWriter(.result = value, 
                     .dbmodel = private$p_dbmodel,
                     .tblname = private$p_tblname,
                     .chunksize = 1e5, 
                     .n = NULL,
                     .ConstructQuery = ConstructReplaceQuery)
    }
  ),
  private = list(
    p_dbmodel = NULL,
    p_tblname = NULL,
    p_tbl = NULL
  )
)

device_list <- tbl_active$new("device_list")

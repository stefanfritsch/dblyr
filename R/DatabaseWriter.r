#' Update or Replace into Database
#' 
#' Will write data to the database, replacing existing data as necessary.
#' .result needs to contain full rows, i.e. all columns.
#' 
#' @param .result data.table with new data
#' @param .dbname Name of the database
#' @param .tblname Name of the table in .connection
#' @param .chunksize Integer. Data will be written chunkwise. Either specify
#'                    the size with this or the number of chunks with .n
#' @param .n Integer. Number of chunks. This overrides .chunksize if both are
#'            specified.
#' 
#' @aliases ReplaceIntoDB, UpdateIntoDB
DatabaseWriter <- function(.result, .dbmodel, .tblname,
                           .chunksize = 1e5, .n = NULL,
                           .ConstructQuery = ConstructReplaceQuery)
{
  ##### Input checks and preparation ###########################################
  stopifnot(
    is.tbl(.result),
    is.character(.tblname),
    is.null(.n) | .n > 0
  )
  
  ##### Compute chunksize ######################################################
  if(!is.null(.n))
  {
    .chunksize <- ceiling(NROW(.result)/.n)
  } else {
    .n <- ceiling(NROW(.result)/.chunksize)
  }
  
  if(".SPLIT" %in% names(.result))
    stop(".SPLIT in column names. Send a bug report.")
  
  .result %<>% mutate(.SPLIT = rep(1:.n, each = .chunksize)[1:n()])
  
  ##### Update DB ##############################################################
  cat("Update ", .tblname, "...\n")
  
  Connections <- Connector$new()
  on.exit(Connections$disconnect())
  
  #---- Loop over chunks ------------------------------------------------------#
  i <- 1
  error.count <- 0
  
  while(i <= .n)
  {
    # We raise the error count here and lower it again at the end if the chunk
    # was written successfully
    if(error.count > 2)
      stop("Too many errors writing to DB")
    
    error.count <- error.count + 1
   
    #---- Write chunk ---------------------------------------------------------#
    cat("Writing chunk ", i, " of ", .n, "\n")
    

    query <- .result %>%
      filter(.SPLIT == i) %>%
      select(-.SPLIT) %>%
      .ConstructQuery(.tblname)
          
    
    try({
      dbGetQuery(.dbmodel$getdb$con, query)
      i <- i+1
      error.count <- error.count - 1
    })
  }
  
  TRUE
}


ConstructReplaceQuery <- function(.result, .tblname)
{
  .result %>%
    # build "VALUES" part of query, i.e. data tuples
    do_on_columns("paste", sep="','") %>%
    paste0("('", ., "')") %>%
    paste0(collapse=",") %>%
    str_replace_all(fixed("'NA'"),"NULL") %>%
    # Combine in query
    str_c("REPLACE INTO ", .tblname, " ",
          " (", paste0(collapse=",", names(.result)), ") ",
          " VALUES ",
          ., 
          ";")
}

ConstructUpdateQuery <- function(.result, .tblname)
{
  cols <- names(.result)
  
  x<-.result %>%
    # build "VALUES" part of query, i.e. data tuples
    do_on_columns("paste", sep="','") %>%
    paste0("('", ., "')") %>%
    paste0(collapse=",") %>%
    str_replace_all(fixed("'NA'"),"NULL") %>%
    str_c("INSERT INTO ", .tblname, " ",
          " (", str_c(collapse=",", cols), ") ",
          " VALUES ",
          .,
          " ON DUPLICATE KEY UPDATE ",
          paste0(collapse=",",
                cols, "=VALUES(", cols,")"),
          ";")
}
#' Connects to the data base
#'
#' This R6 class provides the dplyr data sources sb_aggs, sb_monitor 
#' and pv_smart.
#'
#' @param .host MySQL-db host
#' @param .user MySQL username
#' @param .port MySQL port
#' @param .password MySQL password
#' 
#' @return R6 class with the members
#'  - new(.host, .user, .port, .password)
#'  - disconnect(.dbnames) - .dbnames is a character vector of connections that
#'    should be closed
#'  - disconnect_all() - Closes *all* connections to the server, whether in this
#'      object or not.
#'  - sb_aggs - dplyr source, auto reconnects via active binding
#'  - sb_monitor - dplyr source, auto reconnects via active binding
#'  - statup - dplyr source, auto reconnects via active binding
#'  - sb_detection - dplyr source, auto reconnects via active binding
#'  
#'  @import magrittr
#'  @export
#'  @examples
#'  \dontrun{
#'  Connections<-Connector$new()
#'  Connections$statup
#'  Connections$disconnect()
#'  }
db_connection <- R6::R6Class(
  "DatabaseConnection",
  #*****************************************************************************
  # Public
  #*****************************************************************************
  public = list(
    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------
    initialize = function(.credentials.file,
                          .dbname   = NULL,
                          .host     = NULL,
                          .user     = NULL,
                          .port     = NULL,
                          .password = NULL)
    {
      private$credentials <- .credentials.file
      
      private$update_credentials(.dbname, .host, .user, .port, .password)
    },
    #---------------------------------------------------------------------------
    # disconnect()
    #---------------------------------------------------------------------------
    disconnect = function()
    {
      try({
        lapply(DBI::dbListResults(private$db$con),
               DBI::dbClearResult)
        DBI::dbDisconnect(private$db$con)
      }, silent=TRUE)
    },
    #---------------------------------------------------------------------------
    # disconnect()
    #---------------------------------------------------------------------------
    disconnect_everything = function()
    {
      all_cons<-DBI::dbListConnections(RMySQL::MySQL())
      
      for(con in all_cons)
      {
        try(DBI::dbDisconnect(con), silent=TRUE)
      }
    }
  ),
  #*****************************************************************************
  # Active
  #*****************************************************************************
  active = list(
    getdb = function() private$connect()
  ),
  #*****************************************************************************
  # Private
  #*****************************************************************************
  private = list(
    credentials = NULL,
    update_credentials = function(.dbname   = NULL,
                                  .host     = NULL,
                                  .user     = NULL,
                                  .port     = NULL,
                                  .password = NULL)
    {
      creds.new <- list(
        "dbname" = .dbname, 
        "host" = .host, 
        "user" = .user, 
        "port" = .port, 
        "password" = .password
      )
      
      if(file.exists(private$credentials))
      {
        creds.old <- read.csv(private$credentials, stringsAsFactors = FALSE)
        
        for(i in 1:5)
        {
          i.old <- which(creds.old$cred == names(creds.new)[i])
          
          if(is.null(creds.new[[i]]) && 
               length(i.old) == 1)
          {
            creds.new[[i]] <- creds.old$value[i.old]
          }
        }
      } else {
        message("This will create the file ", private$credentials, 
                " to store the database credentials.\n",
                "Please make sure you have set the access rights to the ", 
                "directory appropriately so that no unauthorized user can ",
                "read the file.")
      }
      
      creds.new <- creds.new %>%
        lapply(
          function(.x)
            if(is.null(.x)) NA else .x 
        ) %>%
        unlist
      
      if(any(is.na(creds.new)))
        stop("The following credentials are missing:\n",
             creds.new %>%
               {names(.)[is.na(.)]} %>%
               paste(collapse = ", "))
      
      
      private$dbname   <- creds.new["dbname"]
      private$host     <- creds.new["host"]
      private$user     <- creds.new["user"]
      private$port     <- creds.new["port"] %>% as.integer
      private$password <- creds.new["password"]
      
      creds.new %>%
        data.frame(cred = names(.), value = .) %>%
        write.csv(private$credentials, row.names = FALSE)
    },
    #---------------------------------------------------------------------------
    # Name of database
    #---------------------------------------------------------------------------
    dbname = NULL,
    #---------------------------------------------------------------------------
    # Connections
    #---------------------------------------------------------------------------
    db = NULL,
    #---------------------------------------------------------------------------
    # Credentials
    #---------------------------------------------------------------------------
    host = NULL, 
    user = NULL, 
    port = NULL, 
    password = NULL,
    #---------------------------------------------------------------------------
    # Connector function
    #---------------------------------------------------------------------------
    connect = function()
    {  
      try({
        DBI::dbListTables(private$db$con)
        return(private$db)
      }, silent=TRUE)
      
      private$db<-dplyr::src_mysql(
        host=private$host,
        user=private$user,
        port=private$port,
        password=private$password,
        dbname=private$dbname
      )
      
      private$db
    }
  )
)

#' do.call on the Columns of a data.frame
#' 
#' If you have a data.frame DF with columns col1 and col2 then 
#' do_on_columns(DF, paste) will execute paste(DF$col1, DF$col2) without 
#' needlessly copying the data.
#' 
#' @param .data A data.table, data.frame, list or similar.
#' @param .f    A function. 
#' 
#' @import magrittr
#' @export
#' @examples
#' DF <- data.frame(col1 = letters[1:5], col2 = 5:1)
#' 
#' do_con_columns(DF, paste)
do_on_columns <- function(.data, .f, ...)
{
  ##### Check inputs ###########################################################
  stopifnot(is.list(.data),
            is.character(.f) || is.function(.f))
  
  if(is.character(.f))
    .f <- as.name(.f)
  
  dots <- eval(substitute(alist(...)))
  
  ##### Construct call and eval ################################################
  names(.data) %>%
    lapply(as.name) %>%
    c(.f, ., dots, recursive = TRUE) %>%
    as.call %>%
    eval(envir = .data, enclos = parent.frame())
}

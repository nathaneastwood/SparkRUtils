#' Parse Schema
#'
#' Given a [SparkR::structType()] schema, parse it into a named vector.
#'
#' @param schema `structType`. The schema to parse.
#'
#' @return A named vector whose name(s) represent the column name(s) and the value(s) represent the Spark data type(s)
#' of the column(s).
#'
#' @noRd
parseSchema <- function(schema) {
  schemaCheck(schema)
  schema <- schema$fields()
  res <- vapply(schema, function(x) x$dataType.simpleString(), NA_character_)
  nms <- vapply(schema, function(x) x$name(), NA_character_)
  setNames(object = res, nm = nms)
}

#' Spark to R Type Mapping
#'
#' A `list` of type mappings taken from [apache/spark](https://github.com/apache/spark/blob/master/R/pkg/R/types.R).
#'
#' @return A named `list` whose names are the Spark types and the entries are the R types.
#' @noRd
sparkToRTypeMapping <- function() {
  list(
    "tinyint" = "integer",
    "smallint" = "integer",
    "byte" = "integer",
    "integer" = "integer",
    "int" = "integer",
    "bigint" = "numeric",
    "float" = "numeric",
    "double" = "numeric",
    "decimal" = "numeric",
    "num" = "numeric",
    "string" = "character",
    "chr" = "character",
    "binary" = "raw",
    "boolean" = "logical",
    "logi" = "logical",
    "timestamp" = c("POSIXct", "POSIXt"),
    "date" = "Date",
    "array" = "list",
    "map" = "environment",
    "struct" = "named list"
  )
}

#' Convert R Data Types to Spark Data Types
#'
#' Given a `data.frame`, convert the column data types into the possible Spark data types.
#'
#' @param df `data.frame`. The data whose column types should be converted.
#'
#' @return A named `list` whose names represent the column names and the values represent the possible Spark data
#' type(s).
#'
#' @noRd
convertRTypes <- function(df) {
  stopifnot(is.data.frame(df))
  sparkToRTypeMapping <- sparkToRTypeMapping()
  dfTypes <- lapply(df, class)
  if (any(dfTypes == "list")) {
    index <- which(dfTypes == "list")
    isNamed <- do.call(c, lapply(df[, index], function(x) !is.null(names(x))))
    dfTypes[isNamed] <- "named list"
  }
  lapply(
    dfTypes,
    function(x) {
      names(which(vapply(sparkToRTypeMapping, identical, NA, x)))
    }
  )
}

#' Schema Check
#'
#' Given a [SparkR::structType()], check for any invalid data types. Invalid data types are those which cannot be
#' converted to R data types.
#'
#' @param schema `structType`. The schema to check.
#'
#' @return If successful the `schema`, invisibly, else errors.
#' @noRd
checkSchema <- function(schema) {
  schema <- parseSchema(schema)
  sparkToRTypeMapping <- list(
    "tinyint" = "integer",
    "smallint" = "integer",
    "byte" = "integer",
    "integer" = "integer",
    "int" = "integer",
    "bigint" = "numeric",
    "float" = "numeric",
    "double" = "numeric",
    "decimal" = "numeric",
    "num" = "numeric",
    "string" = "character",
    "chr" = "character",
    "binary" = "raw",
    "boolean" = "logical",
    "logi" = "logical",
    "timestamp" = c("POSIXct", "POSIXt"),
    "date" = "Date",
    "array" = "list",
    "map" = "environment",
    "struct" = "named list"
  )
  validTypes <- names(sparkToRTypeMapping)
  invalid <- which(!schema %in% validTypes)
  if (any(invalid)) {
    stop(
      "The following spark types are not valid: ", toString(sQuote(schema[invalid], q = FALSE)),
      ". Please choose from one of: ", toString(sQuote(validTypes, q = FALSE)), "."
    )
  }
  invisible(schema)
}

#' Check Schema
#'
#' Given a `data.frame` and a schema, ensure the `data.frame`'s column types match with the specified schema.
#'
#' This function will check three separate things:
#' 1. The number of columns match.
#' 2. The column names match.
#' 3. The possible converted Spark data type(s) of the R columns match the schema.
#'
#' @param df `data.frame`. The data whose column types should be checked.
#' @param schema `character(n)`. The schema to check against where `n` should equal the number of columns in `df`, see
#' [parseSchema()].
#'
#' @return If successful, `df`, else errors.
#' @noRd
checkSchemaAgainstReturnedDataFrame <- function(df, schema) {
  stopifnot(is.data.frame(df))
  ctypes <- convertRTypes(df)

  if (length(schema) != length(ctypes)) {
    stop(
      "The number of columns specified in the schema and the returned data.frame are different:\n",
      "    schema: ", toString(sQuote(names(schema), q = FALSE)), "\n",
      "    data.frame: ", toString(sQuote(names(ctypes), q = FALSE))
    )
  }

  if (any(names(schema) != names(ctypes))) {
    stop(
      "The specified column names in the schema do not match the names of the returned data.frame:\n",
      "    schema: ", toString(sQuote(names(schema), q = FALSE)), "\n",
      "    data.frame: ", toString(sQuote(names(ctypes), q = FALSE))
    )
  }

  errors <- rep(FALSE, length(schema))
  for (i in seq_along(schema)) {
    errors[i] <- !schema[i] %in% unlist(ctypes[which(names(schema[i]) == names(ctypes))])
  }
  if (any(errors)) {
    errs <- which(errors)
    msg <- rep(NA_character_, length(errs))
    for (i in seq_along(errs)) {
      ctypeErrors <- unlist(ctypes[errs[i]])
      correctTypeMsg <- if (length(ctypeErrors) > 1L) " be one of " else " be "
      msg[i] <- paste0(
        "    The schema specified that the column ", sQuote(names(schema)[errs[i]], q = FALSE),
        " should be of type ", sQuote(schema[errs[i]], q = FALSE), ", however the column is of type ",
        sQuote(class(df[[errs[i]]]), q = FALSE), ". Therefore the schema type should", correctTypeMsg,
        toString(sQuote(ctypeErrors, q = FALSE)), "."
      )
      if (i < max(length(errs))) msg[i] <- paste0(msg[i], ".\n")
    }
    stop("Differences detected in the schema and data.frame output:\n", msg)
  }
  return(df)
}

#' Safe `gapply`
#'
#' A simple wrapper around [SparkR::gapply()] which provides the following safety checks:
#' 1. The specified schema contains data types which can be converted to from the R types, otherwise fails early.
#' 2. The number of columns match between the node's returned `data.frame` and the schema.
#' 3. The column names match between the node's returned `data.frame` and the schema.
#' 4. The column type(s) of the node's returned `data.frame` match with the column type(s) specified in the schema.
#'
#' @param df A `SparkDataFrame`.
#' @param fn A `function` to run against each group of data. The function must use `df` as its first argument.
#' @param groups `character(n)`. The names of the columns to group over.
#' @param schema `character(1)`. The schema which represents the final Spark DataFrame output.
#'
#' @return A `SparkDataFrame`.
#'
#' @examples
#' \dontrun{
#' sparkR.session()
#'
#' data <- as.DataFrame(
#'   data.frame(
#'     dateTime = rep(
#'       seq(from = as.POSIXct("2020-01-01 00:00:00"), to = as.POSIXct("2020-01-01 00:01:50"), by = "10 s"),
#'       2
#'     ),
#'     group = rep(c("group_1", "group_2"), each = 6),
#'     value = c(rnorm(6), runif(6))
#'   )
#' )
#'
#' # The following fails because value should be a double
#' collect(sgapply(
#'   df = data,
#'   groups = "group",
#'   fn = function(df) {
#'     df$mean_value <- mean(df$value, na.rm = TRUE)
#'     df
#'   },
#'   schema = "dateTime timestamp, group string, value integer, mean_value double"
#' ))
#'
#' # The following fails before computation because value contains a Spark data type which cannot be converted.
#' collect(sgapply(
#'   df = data,
#'   groups = "group",
#'   fn = function(df) {
#'     df$mean_value <- mean(df$value, na.rm = TRUE)
#'     df
#'   },
#'   schema = "dateTime timestamp, group string, value decimal(38,6), mean_value double"
#' ))
#' }
#'
#' @importFrom SparkR gapply
#' @export
sgapply <- function(df, groups, fn, schema) {
  stopifnot(inherits(df, "SparkDataFrame"))
  stopifnot(is.character(groups))
  stopifnot(is.function(fn))
  parsedSchema <- parseSchema(schema)
  checkSchema(schema)
  SparkR::gapply(
    x = df,
    cols = groups,
    func = function(key = groups, x = df, ...) {
      res <- fn(df = x, ...)
      checkSchemaAgainstReturnedDataFrame(res, parsedSchema)
      res
    },
    schema = schema
  )
}

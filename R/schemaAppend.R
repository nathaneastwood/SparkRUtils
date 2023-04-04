#' Append Additional Fields to a Schema
#'
#' Given a DataFrame's schema, append additional fields to it.
#'
#' @param schema `structType`. A DataFrame's schema, see [SparkR::schema()].
#' @param fields `list()`. A `list` of [SparkR::structField()]s to append to the
#' end of the DataFrame.
#'
#' @examples
#' \dontrun{
#' x <- SparkR::sql("SELECT * FROM tbl")
#' new_col <- SparkR::structField("new_col", "double")
#' schemaAppend(
#'   schema = SparkR::schema(x),
#'   fields = list(new_col)
#' )
#' })
#'
#' @seealso [SparkR::structType()], [SparkR::structField()], [SparkR::schema()]
#'
#' @importFrom SparkR structType
#'
#' @export
schemaAppend <- function(schema, fields) {
  stopifnot(inherits(x = schema, what = "structType"))
  stopifnot(is.list(fields))
  stopifnot(all(vapply(
    X = fields,
    FUN = function(f) inherits(x = f, what = "structField"),
    FUN.VALUE = logical(1L)
  )))
  schema <- do.call(SparkR::structType, c(schema$fields(), fields))
}

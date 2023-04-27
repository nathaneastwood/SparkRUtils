#' Schema Modifiers
#'
#' @param schema `structType`. A DataFrame's schema, see [SparkR::schema()].
#'
#' @examples
#' \dontrun{
#' x <- SparkR::sql("SELECT * FROM tbl")
#' newCol <- SparkR::structField("newCol", "double")
#' newSchema <- schemaAppend(
#'   schema = SparkR::schema(x),
#'   fields = list(newCol)
#' )
#' newSchema
#'
#' schemaRemove(
#'   schema = newSchema,
#'   remove = "newCol"
#' )
#' }
#'
#' @seealso [SparkR::structType()], [SparkR::structField()], [SparkR::schema()]
#'
#' @importFrom SparkR structType
#'
#' @name schema

#' @description *schemaAppend*: Given a DataFrame's schema, append additional
#' fields to it.
#' @param append `list()`. A `list` of [SparkR::structField()]s to append to the
#' end of the DataFrame.
#' @rdname schema
#' @export
schemaAppend <- function(schema, append) {
  schemaCheck(schema)
  stopifnot(is.list(append))
  fieldCheck(append)
  do.call(SparkR::structType, c(schema$fields(), append))
}

#' @description *schemaRemove*: Given a DataFrame's schema, remove fields from
#' it.
#' @param remove `character(n)`. A vector of field names to be removed from the
#' `schema`.
#' @rdname schema
#' @export
schemaRemove <- function(schema, remove) {
  schemaCheck(schema)
  stopifnot(is.character(remove))
  fields <- schema$fields()
  fieldNames <- getSchemaFieldNames(fields)
  do.call(SparkR::structType, fields[!fieldNames %in% remove])
}

getSchemaFieldNames <- function(fields) {
  vapply(fields, function(x) x$name(), NA_character_)
}

schemaCheck <- function(schema) {
  stopifnot(inherits(x = schema, what = "structType"))
}

fieldCheck <- function(fields) {
  stopifnot(all(vapply(
    X = fields,
    FUN = function(f) inherits(x = f, what = "structField"),
    FUN.VALUE = logical(1L)
  )))
}

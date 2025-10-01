# transformJsonTableColumn

## Location
[src/backend/parser/parse_jsontable.c:399-453](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_jsontable.c#L399-L453)

## Overview
Transforms a JSON_TABLE column definition into a JsonFuncExpr node, converting regular columns to JSON_VALUE(), FORMAT JSON columns to JSON_QUERY(), and EXISTS columns to JSON_EXISTS().

## Definition
```c
static JsonFuncExpr *transformJsonTableColumn(JsonTableColumn *jtc, Node *contextItemExpr, List *passingArgs)
```

## Detailed Description
This static function is responsible for converting parsed JSON table column specifications into executable JSON function expressions. It determines the appropriate JSON operation type based on the column type and constructs the corresponding JsonFuncExpr node with proper path specifications, output formatting, and error handling behaviors.

The function handles three main column types:
- Regular columns: Converted to JSON_VALUE() operations for scalar value extraction
- FORMAT JSON columns: Converted to JSON_QUERY() operations for composite value extraction  
- EXISTS columns: Converted to JSON_EXISTS() operations for boolean existence checks

When no explicit path is specified, it automatically generates a default JSONPath expression using the column name in the format `$.\"column_name\"`.

## Parameters / Member Variables
- `jtc`: The JsonTableColumn structure containing the column definition to transform
- `contextItemExpr`: The context item expression that provides the JSON data source
- `passingArgs`: List of arguments to be passed to the JSON function

## Dependencies
- Functions called/Symbols referenced:
  - makeNode: Creates new PostgreSQL parse tree nodes
  - [makeJsonValueExpr](../m/makeJsonValueExpr.md): Creates JSON value expressions with format specifications
  - [makeJsonFormat](../m/makeJsonFormat.md): Creates JSON format specifications with encoding defaults
  - [makeStringConst](../m/makeStringConst.md): Creates string constant nodes
  - [escape_json](../e/escape_json.md): Properly escapes JSON identifiers
  - [pstrdup](../p/pstrdup.md): Duplicates strings in the current memory context
  - [initStringInfo](../i/initStringInfo.md)/appendStringInfoString: String buffer operations

- Called from (representative examples):
  - [transformJsonTableColumns](transformJsonTableColumns.md): Main entry point that processes all columns in a JSON table

## Notes and Other Information
- The function preserves error handling behaviors (on_empty, on_error) from the original column definition
- Default path generation ensures valid JSONPath syntax by properly escaping column names
- The location information is preserved for better error reporting during execution
- Column names are stored for runtime error context in JsonExpr operations

## Simplified Source

```c
static JsonFuncExpr *
transformJsonTableColumn(JsonTableColumn *jtc, Node *contextItemExpr, List *passingArgs)
{
    JsonFuncExpr *jfexpr = makeNode(JsonFuncExpr);

    // Choose JSON operation type based on column type
    if (jtc->coltype == JTC_REGULAR)
        jfexpr->op = JSON_VALUE_OP;
    else if (jtc->coltype == JTC_EXISTS)
        jfexpr->op = JSON_EXISTS_OP;
    else
        jfexpr->op = JSON_QUERY_OP;

    // Store column name for error reporting
    jfexpr->column_name = pstrdup(jtc->name);

    // Set up context item with JSON format defaults
    jfexpr->context_item = makeJsonValueExpr((Expr *) contextItemExpr, NULL,
                                           makeJsonFormat(JS_FORMAT_DEFAULT, JS_ENC_DEFAULT, -1));

    // Use specified path or generate default path: $.\"column_name\"
    if (jtc->pathspec) {
        jfexpr->pathspec = (Node *) jtc->pathspec->string;
    } else {
        StringInfoData path;
        initStringInfo(&path);
        appendStringInfoString(&path, "$.");
        escape_json(&path, jtc->name);
        jfexpr->pathspec = makeStringConst(path.data, -1);
    }

    // Copy remaining properties from column definition
    jfexpr->passing = passingArgs;
    jfexpr->output = makeNode(JsonOutput);
    jfexpr->output->typeName = jtc->typeName;
    jfexpr->output->returning = makeNode(JsonReturning);
    jfexpr->output->returning->format = jtc->format;
    jfexpr->on_empty = jtc->on_empty;
    jfexpr->on_error = jtc->on_error;
    jfexpr->quotes = jtc->quotes;
    jfexpr->wrapper = jtc->wrapper;
    jfexpr->location = jtc->location;

    return jfexpr;
}
```
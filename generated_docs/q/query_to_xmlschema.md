# query_to_xmlschema

## Location
[src/backend/utils/adt/xml.c:3065-3093](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L3065-L3093)

## Overview
SQL-callable function that generates an XML Schema Definition (XSD) based on the result structure of a given SQL query.

## Definition

```c
Datum
query_to_xmlschema(PG_FUNCTION_ARGS)
```
## Detailed Description
This function executes a SQL query and generates an XML Schema Definition that describes the structure of the query's result set. Unlike table_to_xmlschema which works with existing tables, this function can generate schemas for arbitrary SELECT queries, joins, computed columns, and complex result sets. It uses PostgreSQL's cursor mechanism to analyze the query result structure without actually fetching the data, making it efficient for schema generation purposes.

## Parameters / Member Variables
- `PG_GETARG_TEXT_PP(0)`: SQL query string to analyze
- `PG_GETARG_BOOL(1)`: Boolean flag for including null value information in schema
- `PG_GETARG_BOOL(2)`: Boolean flag for table forest format vs single table format  
- `PG_GETARG_TEXT_PP(3)`: Target namespace for the XML schema

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring
  - PG_GETARG_BOOL
  - SPI_connect
  - [SPI_prepare](../S/SPI_prepare.md)
  - [SPI_cursor_open](../S/SPI_cursor_open.md)
  - [_SPI_strdup](../S/_SPI_strdup.md)
  - [map_sql_table_to_xmlschema](../m/map_sql_table_to_xmlschema.md)
  - [SPI_cursor_close](../S/SPI_cursor_close.md)
  - SPI_finish
  - [cstring_to_xmltype](../c/cstring_to_xmltype.md)
  - PG_RETURN_XML_P
- Called from:
  - Available as SQL function (no direct C callers found)

## Notes and Other Information
- Function is exposed to SQL layer as a built-in function
- Uses SPI cursor mechanism to analyze query structure without executing it fully
- More flexible than table_to_xmlschema as it works with any valid SELECT query
- Handles complex queries including joins, subqueries, and computed columns
- Uses InvalidOid as table OID since this represents a query result rather than a specific table
- Memory management includes _SPI_strdup for safe string duplication
- Part of PostgreSQL's comprehensive XML support for dynamic schema generation
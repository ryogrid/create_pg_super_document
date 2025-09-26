# database_to_xmlschema

## Location
[src/backend/utils/adt/xml.c:3454-3465](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L3454-L3465)

## Overview
A PostgreSQL SQL-callable function that generates an XML Schema definition for all visible tables and schemas in the current database.

## Definition
```c
Datum
database_to_xmlschema(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the SQL interface for generating XML Schema definitions of the entire database structure. It extracts function arguments from the PostgreSQL function call interface, processes them into appropriate C types, and delegates the actual schema generation to the internal implementation. The function converts the resulting StringInfo into an XML data type suitable for return to SQL clients.

## Parameters / Member Variables
The function receives arguments through PG_FUNCTION_ARGS containing:
- Argument 0: `nulls` (boolean) - Whether to include nullable elements in the schema
- Argument 1: `tableforest` (boolean) - Controls XML structure format (table forest vs. single table)
- Argument 2: `targetns` (text) - Target namespace for the generated XML Schema

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOOL
  - [text_to_cstring](../t/text_to_cstring.md)
  - [database_to_xmlschema_internal](database_to_xmlschema_internal.md)
  - [stringinfo_to_xmltype](../s/stringinfo_to_xmltype.md)
  - PG_RETURN_XML_P
- Called from:
  - No direct references (SQL-callable function)

## Notes and Other Information
- This is a PostgreSQL built-in function accessible from SQL
- Acts as a wrapper around database_to_xmlschema_internal
- Handles PostgreSQL function call protocol and argument conversion
- Returns XML data type that can be used directly in SQL queries
- Part of PostgreSQL's XML functionality for schema introspection
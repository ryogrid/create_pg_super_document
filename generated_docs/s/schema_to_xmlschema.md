# schema_to_xmlschema

## Location
src/backend/utils/adt/xml.c: 3315 - 3327

## Overview
PostgreSQL SQL function wrapper that generates XML Schema (XSD) definition for all visible tables in a specified database schema, providing a user-accessible interface to schema-to-XSD conversion functionality.

## Definition
```c
Datum schema_to_xmlschema(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the PostgreSQL SQL function entry point for converting a database schema to its corresponding XML Schema (XSD) representation. It extracts function arguments using PostgreSQL's function call macros, processes the schema name, null handling flag, table forest option, and target namespace parameter, then delegates the actual conversion work to schema_to_xmlschema_internal. The result is converted from StringInfo to PostgreSQL's internal XML type and returned to the SQL caller.

## Parameters / Member Variables
- Function arguments via PG_FUNCTION_ARGS:
  - Argument 0: `name` (Name type) - PostgreSQL schema name to convert
  - Argument 1: `nulls` (bool) - Whether to include nullable information in XSD  
  - Argument 2: `tableforest` (bool) - Controls XML structure format
  - Argument 3: `targetns` (text) - Target namespace URI for generated XML Schema

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME
  - PG_GETARG_BOOL
  - text_to_cstring
  - schema_to_xmlschema_internal
  - stringinfo_to_xmltype
  - PG_RETURN_XML_P
- Called from (representative examples):
  - (No direct references found - likely called via SQL function interface)

## Notes and Other Information
This is a PostgreSQL C function that can be called from SQL using the schema_to_xmlschema() function. It follows PostgreSQL's function calling conventions using the PG_FUNCTION_ARGS macro and related argument extraction macros. The function handles type conversion between PostgreSQL internal types (Name, text) and C types (char*) before calling the internal implementation.
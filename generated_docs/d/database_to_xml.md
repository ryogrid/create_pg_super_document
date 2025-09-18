# database_to_xml

## Location
[src/backend/utils/adt/xml.c:3399-3410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L3399-L3410)

## Overview
PostgreSQL SQL function wrapper that generates XML representation of an entire database by converting all visible schemas and their tables to XML format without XML Schema definition.

## Definition
```c
Datum database_to_xml(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the PostgreSQL SQL function entry point for converting an entire database to its XML representation. It extracts function arguments for null handling, table forest formatting, and target namespace, then delegates the actual conversion work to database_to_xml_internal with NULL for the xmlschema parameter, indicating that no XML Schema definition should be included in the output. The result is converted from StringInfo to PostgreSQL's internal XML type and returned to the SQL caller, providing a complete XML representation of all visible database schemas and their tables.

## Parameters / Member Variables
- Function arguments via PG_FUNCTION_ARGS:
  - Argument 0: `nulls` (bool) - Whether to include null values in the XML output
  - Argument 1: `tableforest` (bool) - Controls XML structure format (forest vs document)
  - Argument 2: `targetns` (text) - Target namespace URI for generated XML elements

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOOL
  - text_to_cstring
  - [database_to_xml_internal](database_to_xml_internal.md)
  - [stringinfo_to_xmltype](../s/stringinfo_to_xmltype.md)
  - PG_RETURN_XML_P
- Called from (representative examples):
  - (No direct references found - likely called via SQL function interface)

## Notes and Other Information
This is a PostgreSQL C function that can be called from SQL using the database_to_xml() function. It follows PostgreSQL's function calling conventions and provides a way to export an entire database structure and data to XML format. Unlike database_to_xml_and_xmlschema, this function does not include XML Schema definition in the output, producing only the XML data representation. The function is part of PostgreSQL's SQL/XML functionality for XML data generation and export.
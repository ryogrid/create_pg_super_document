# schema_to_xml

## Location
src/backend/utils/adt/xml.c: 3224 - 3245

## Overview
PostgreSQL built-in function that converts a schema (namespace) to XML format without including XML schema definition.

## Definition


## Detailed Description
This function is a PostgreSQL built-in function that provides a SQL interface for converting an entire database schema to XML format. It takes a schema name and converts all visible tables within that schema to XML representation. The function acts as a wrapper around schema_to_xml_internal, handling parameter extraction and result formatting.

The function:
1. Extracts the schema name from the function arguments
2. Resolves the schema name to its internal object ID (nspid)
3. Calls schema_to_xml_internal to perform the actual conversion
4. Returns the result as an XML type

This is the simpler version that does not include XML schema definitions, in contrast to schema_to_xml_and_xmlschema.

## Parameters / Member Variables
- : Name of the schema to convert to XML
- : nulls - whether to include NULL values in the XML output
- : tableforest - whether to format output as table forest structure
- : targetns - target namespace for the XML output

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME
  - PG_GETARG_BOOL
  - text_to_cstring
  - LookupExplicitNamespace
  - stringinfo_to_xmltype
  - schema_to_xml_internal
  - PG_RETURN_XML_P
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's function call mechanism)

## Notes and Other Information
- This function is designed to be called from SQL as a built-in function
- Uses LookupExplicitNamespace to safely resolve schema names to OIDs
- Passes NULL as the xmlschema parameter to schema_to_xml_internal, indicating no schema definition should be included
- Part of PostgreSQL's SQL/XML compliance implementation
- Located in src/backend/utils/adt/xml.c:3224-3245
- Companion function to schema_to_xml_and_xmlschema which includes schema definitions
# schema_to_xml_internal

## Location
src/backend/utils/adt/xml.c: 3181 - 3223

## Overview
Internal function that converts a PostgreSQL schema (namespace) to XML format, including all visible tables within the schema and optionally an XML schema definition.

## Definition


## Detailed Description
This internal function implements the core logic for mapping a SQL schema to XML format as specified in SQL/XML:2008 sections 9.13 and 9.14. It processes a PostgreSQL namespace (schema) by:

1. Converting the schema name to XML-compliant format
2. Creating a root XML element with optional schema information
3. Retrieving all XML-visible tables within the schema
4. Converting each table to XML format using table_to_xml_internal
5. Assembling the results into a complete XML document

The function supports both simple XML output and XML Schema-aware output, and can format results as either regular XML or table forest structure.

## Parameters / Member Variables
- : Object ID of the PostgreSQL namespace (schema) to convert
- : Optional XML schema definition to include in the output
- : Whether to include NULL values in the XML representation
- : Whether to format output using table forest structure
- : Target namespace for the XML output
- : Whether this is a top-level conversion (affects XML structure)

## Dependencies
- Functions called/Symbols referenced:
  - map_sql_identifier_to_xml_name
  - get_namespace_name
  - makeStringInfo
  - xmldata_root_element_start
  - SPI_connect
  - schema_get_xml_visible_tables
  - table_to_xml_internal
  - appendBinaryStringInfo
  - SPI_finish
  - xmldata_root_element_end
- Called from (representative examples):
  - schema_to_xml
  - schema_to_xml_and_xmlschema
  - database_to_xml_internal

## Notes and Other Information
- This is a static internal function, not directly accessible from SQL
- Uses SPI for database operations to retrieve table information
- Follows SQL/XML:2008 standard specifications
- Creates XML with proper namespace handling and schema inclusion
- Located in src/backend/utils/adt/xml.c:3181-3223
- Part of PostgreSQL's XML support infrastructure
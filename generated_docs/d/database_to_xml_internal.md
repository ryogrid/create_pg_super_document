# database_to_xml_internal

## Location
src/backend/utils/adt/xml.c: 3356 - 3398

## Overview
Internal function that generates XML representation of an entire PostgreSQL database by converting all visible schemas and their tables to XML format, with optional XML Schema integration.

## Definition
```c
static StringInfo database_to_xml_internal(const char *xmlschema, bool nulls, bool tableforest, const char *targetns)
```

## Detailed Description
This function creates a comprehensive XML representation of a PostgreSQL database by iterating through all visible schemas and converting each to XML using schema_to_xml_internal. It establishes a root XML element named after the current database, optionally includes XML Schema definition at the top, opens an SPI connection to access database metadata, retrieves all visible schemas, and processes each schema individually to build the complete database XML. The function follows SQL/XML:2008 standards (sections 9.16, 9.17) for database-to-XML mapping and properly formats the output with appropriate root element wrapping and namespace handling.

## Parameters / Member Variables
- `xmlschema`: Optional XML Schema definition string to include in the output (can be NULL)
- `nulls`: Boolean flag indicating whether to include null values in the XML output
- `tableforest`: Boolean flag controlling XML structure format (table forest vs document structure)
- `targetns`: Target namespace URI to use for the generated XML elements

## Dependencies
- Functions called/Symbols referenced:
  - get_database_name
  - map_sql_identifier_to_xml_name
  - makeStringInfo
  - xmldata_root_element_start
  - SPI_connect
  - database_get_xml_visible_schemas
  - schema_to_xml_internal
  - appendBinaryStringInfo
  - SPI_finish
  - xmldata_root_element_end
- Called from (representative examples):
  - database_to_xml
  - database_to_xml_and_xmlschema

## Notes and Other Information
This is a static internal function that handles the core logic for converting an entire database to XML. It uses SPI for database access and requires proper connection management. The function references SQL/XML:2008 standards for database-to-XML mapping. It creates a hierarchical XML structure with the database as the root element and all visible schemas as child elements. The MyDatabaseId global variable is used to get the current database name for the root element.
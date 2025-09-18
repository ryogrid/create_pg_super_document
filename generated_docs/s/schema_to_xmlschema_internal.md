# schema_to_xmlschema_internal

## Location
src/backend/utils/adt/xml.c: 3270 - 3314

## Overview
Internal function that generates XML Schema (XSD) definition for all visible tables in a PostgreSQL schema, providing the core implementation for schema-to-XSD conversion functionality.

## Definition
```c
static StringInfo schema_to_xmlschema_internal(const char *schemaname, bool nulls, bool tableforest, const char *targetns)
```

## Detailed Description
This function creates an XML Schema (XSD) definition that describes the structure of all visible tables within a specified PostgreSQL database schema. It opens an SPI connection, retrieves all accessible tables in the schema, extracts their tuple descriptors, and converts both the SQL data types and table structures to corresponding XML Schema types and elements. The function generates a complete XSD document with proper schema element wrapping and namespace handling.

## Parameters / Member Variables
- `schemaname`: Name of the PostgreSQL schema to convert to XML Schema
- `nulls`: Boolean flag indicating whether to include nullable information in the XSD
- `tableforest`: Boolean flag controlling the XML structure format (table forest vs individual elements)
- `targetns`: Target namespace URI to use in the generated XML Schema

## Dependencies
- Functions called/Symbols referenced:
  - makeStringInfo
  - [LookupExplicitNamespace](../L/LookupExplicitNamespace.md)
  - [xsd_schema_element_start](../x/xsd_schema_element_start.md)
  - SPI_connect
  - [schema_get_xml_visible_tables](schema_get_xml_visible_tables.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - map_sql_typecoll_to_xmlschema_types
  - map_sql_schema_to_xmlschema_types
  - [xsd_schema_element_end](../x/xsd_schema_element_end.md)
  - SPI_finish
- Called from (representative examples):
  - [schema_to_xmlschema](schema_to_xmlschema.md)
  - [schema_to_xml_and_xmlschema](schema_to_xml_and_xmlschema.md)

## Notes and Other Information
This is a static internal function that handles the core logic for XML Schema generation. It uses the SPI (Server Programming Interface) to access database metadata and requires proper connection management. The function builds the XSD incrementally using StringInfo, first adding type definitions from tuple descriptors, then schema-specific type mappings, all wrapped within proper XSD schema element tags.
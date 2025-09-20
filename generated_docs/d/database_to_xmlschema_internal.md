# database_to_xmlschema_internal

## Location
[src/backend/utils/adt/xml.c:3411-3453](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L3411-L3453)

## Overview
Generates an XML Schema definition for all visible tables and schemas in the current database, serving as the internal implementation for database XML schema generation functions.

## Definition

```c
static StringInfo
database_to_xmlschema_internal(bool nulls, bool tableforest,
							   const char *targetns)
```
## Detailed Description
This internal function creates a complete XML Schema (XSD) representation of the entire database structure. It retrieves all visible tables and schemas from the database, processes their tuple descriptors to extract type information, and generates corresponding XML Schema elements and type definitions. The function establishes an SPI connection to access the database catalog, collects metadata about tables and schemas, and delegates the actual XML Schema generation to specialized mapping functions.

## Parameters / Member Variables
- : Boolean flag indicating whether to include nullable elements in the schema
- : Boolean flag controlling the XML structure format (table forest vs. single table format)
- : Target namespace for the generated XML Schema (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - makeStringInfo
  - [xsd_schema_element_start](../x/xsd_schema_element_start.md)
  - SPI_connect
  - [database_get_xml_visible_tables](database_get_xml_visible_tables.md)
  - [database_get_xml_visible_schemas](database_get_xml_visible_schemas.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - map_sql_typecoll_to_xmlschema_types
  - map_sql_catalog_to_xmlschema_types
  - [xsd_schema_element_end](../x/xsd_schema_element_end.md)
  - SPI_finish
- Called from:
  - [database_to_xmlschema](database_to_xmlschema.md)
  - [database_to_xml_and_xmlschema](database_to_xml_and_xmlschema.md)

## Notes and Other Information
- This is a static function used internally within the XML module
- Requires SPI connection for accessing database metadata
- Handles memory management by creating tuple descriptor copies
- Generates both type collections and catalog schema mappings
- Ensures proper XML Schema structure with start and end elements
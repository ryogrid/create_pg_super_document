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
- `nulls`: Boolean flag indicating whether to include nullable elements in the schema
- `tableforest`: Boolean flag controlling the XML structure format (table forest vs. single table format)
- `*targetns`: Target namespace for the generated XML Schema (can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - [makeStringInfo](../m/makeStringInfo.md)
  - [xsd_schema_element_start](../x/xsd_schema_element_start.md)
  - [SPI_connect](../S/SPI_connect.md)
  - [database_get_xml_visible_tables](database_get_xml_visible_tables.md)
  - [database_get_xml_visible_schemas](database_get_xml_visible_schemas.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - [map_sql_typecoll_to_xmlschema_types](../m/map_sql_typecoll_to_xmlschema_types.md)
  - [map_sql_catalog_to_xmlschema_types](../m/map_sql_catalog_to_xmlschema_types.md)
  - [xsd_schema_element_end](../x/xsd_schema_element_end.md)
  - [SPI_finish](../S/SPI_finish.md)
- Called from:
  - [database_to_xmlschema](database_to_xmlschema.md)
  - [database_to_xml_and_xmlschema](database_to_xml_and_xmlschema.md)

## Notes and Other Information
- This is a static function used internally within the XML module
- Requires SPI connection for accessing database metadata
- Handles memory management by creating tuple descriptor copies
- Generates both type collections and catalog schema mappings
- Ensures proper XML Schema structure with start and end elements

## Simplified Source

```c
static StringInfo
database_to_xmlschema_internal(bool nulls, bool tableforest,
                               const char *targetns)
{
    StringInfo result = makeStringInfo();

    // Start XSD schema element
    xsd_schema_element_start(result, targetns);

    // Connect to SPI for database access
    SPI_connect();

    // Get all visible tables and schemas in database
    List *relid_list = database_get_xml_visible_tables();
    List *nspid_list = database_get_xml_visible_schemas();

    // Extract tuple descriptors from all tables
    List *tupdesc_list = NIL;
    ListCell *cell;
    foreach(cell, relid_list)
    {
        Relation rel = table_open(lfirst_oid(cell), AccessShareLock);
        tupdesc_list = lappend(tupdesc_list, CreateTupleDescCopy(rel->rd_att));
        table_close(rel, NoLock);
    }

    // Add type collection mappings to schema
    appendStringInfoString(result,
                          map_sql_typecoll_to_xmlschema_types(tupdesc_list));

    // Add catalog mappings to schema
    appendStringInfoString(result,
                          map_sql_catalog_to_xmlschema_types(nspid_list, nulls,
                                                            tableforest, targetns));

    // Close XSD schema element
    xsd_schema_element_end(result);

    SPI_finish();
    return result;
}
```
# schema_to_xml_internal

## Location
[src/backend/utils/adt/xml.c:3181-3223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L3181-L3223)

## Overview
Internal function that converts a PostgreSQL schema (namespace) to XML format, including all visible tables within the schema and optionally an XML schema definition.

## Definition

```c
static StringInfo
schema_to_xml_internal(Oid nspid, const char *xmlschema, bool nulls,
					   bool tableforest, const char *targetns, bool top_level)
```
## Detailed Description
This internal function implements the core logic for mapping a SQL schema to XML format as specified in SQL/XML:2008 sections 9.13 and 9.14. It processes a PostgreSQL namespace (schema) by:

1. Converting the schema name to XML-compliant format
2. Creating a root XML element with optional schema information
3. Retrieving all XML-visible tables within the schema
4. Converting each table to XML format using table_to_xml_internal
5. Assembling the results into a complete XML document

The function supports both simple XML output and XML Schema-aware output, and can format results as either regular XML or table forest structure.

## Parameters / Member Variables
- `nspid`: Object ID of the PostgreSQL namespace (schema) to convert
- `*xmlschema`: Optional XML schema definition to include in the output
- `nulls`: Whether to include NULL values in the XML representation
- `tableforest`: Whether to format output using table forest structure
- `*targetns`: Target namespace for the XML output
- `top_level`: Whether this is a top-level conversion (affects XML structure)
## Dependencies
- Functions called/Symbols referenced:
  - [map_sql_identifier_to_xml_name](../m/map_sql_identifier_to_xml_name.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - [makeStringInfo](../m/makeStringInfo.md)
  - [xmldata_root_element_start](../x/xmldata_root_element_start.md)
  - [SPI_connect](../S/SPI_connect.md)
  - [schema_get_xml_visible_tables](schema_get_xml_visible_tables.md)
  - [table_to_xml_internal](../t/table_to_xml_internal.md)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md)
  - [SPI_finish](../S/SPI_finish.md)
  - [xmldata_root_element_end](../x/xmldata_root_element_end.md)
- Called from (representative examples):
  - [schema_to_xml](schema_to_xml.md)
  - [schema_to_xml_and_xmlschema](schema_to_xml_and_xmlschema.md)
  - [database_to_xml_internal](../d/database_to_xml_internal.md)

## Notes and Other Information
- This is a static internal function, not directly accessible from SQL
- Uses SPI for database operations to retrieve table information
- Follows SQL/XML:2008 standard specifications
- Creates XML with proper namespace handling and schema inclusion
- Located in src/backend/utils/adt/xml.c:3181-3223
- Part of PostgreSQL's XML support infrastructure

## Simplified Source

```c
static StringInfo
schema_to_xml_internal(Oid nspid, const char *xmlschema, bool nulls,
                       bool tableforest, const char *targetns, bool top_level)
{
    // Convert schema name to XML-safe format
    char *xmlsn = map_sql_identifier_to_xml_name(get_namespace_name(nspid), true, false);
    StringInfo result = makeStringInfo();

    // Create XML root element with optional schema
    xmldata_root_element_start(result, xmlsn, xmlschema, targetns, top_level);
    appendStringInfoChar(result, '\n');

    // Include XML schema if provided
    if (xmlschema)
        appendStringInfo(result, "%s\n\n", xmlschema);

    // Connect to SPI to access table information
    SPI_connect();

    // Get all visible tables in the schema
    List *relid_list = schema_get_xml_visible_tables(nspid);

    // Convert each table to XML and append to result
    ListCell *cell;
    foreach(cell, relid_list)
    {
        Oid relid = lfirst_oid(cell);
        StringInfo subres = table_to_xml_internal(relid, NULL, nulls,
                                                 tableforest, targetns, false);
        appendBinaryStringInfo(result, subres->data, subres->len);
        appendStringInfoChar(result, '\n');
    }

    SPI_finish();

    // Close XML root element
    xmldata_root_element_end(result, xmlsn);

    return result;
}
```
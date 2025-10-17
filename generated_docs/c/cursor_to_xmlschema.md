# cursor_to_xmlschema

## Location
[src/backend/utils/adt/xml.c:3094-3123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L3094-L3123)

## Overview
SQL-callable function that generates an XML Schema Definition (XSD) based on the result structure of an existing named cursor.

## Definition

```c
Datum
cursor_to_xmlschema(PG_FUNCTION_ARGS)
```
## Detailed Description
This function generates an XML Schema Definition for the result set structure of a previously declared and opened cursor. It looks up an existing named cursor by name and analyzes its tuple descriptor to create the appropriate XML schema. This is particularly useful for applications that need to generate schemas for cursors that may represent complex queries, stored procedures, or other database operations that return result sets.

## Parameters / Member Variables
- `PG_GETARG_TEXT_PP(0)`: Name of the cursor to analyze
- `PG_GETARG_BOOL(1)`: Boolean flag for including null value information in schema
- `PG_GETARG_BOOL(2)`: Boolean flag for table forest format vs single table format
- `PG_GETARG_TEXT_PP(3)`: Target namespace for the XML schema

## Dependencies
- Functions called/Symbols referenced:
  - [text_to_cstring](../t/text_to_cstring.md)
  - PG_GETARG_BOOL
  - [SPI_connect](../S/SPI_connect.md)
  - [SPI_cursor_find](../S/SPI_cursor_find.md)
  - [_SPI_strdup](../S/_SPI_strdup.md)
  - [map_sql_table_to_xmlschema](../m/map_sql_table_to_xmlschema.md)
  - [SPI_finish](../S/SPI_finish.md)
  - [cstring_to_xmltype](cstring_to_xmltype.md)
  - PG_RETURN_XML_P
- Called from:
  - Available as SQL function (no direct C callers found)

## Notes and Other Information
- Function is exposed to SQL layer as a built-in function
- Requires an existing named cursor to be declared and opened prior to calling
- Provides comprehensive error handling for non-existent cursors and invalid cursor states
- Validates that the cursor actually returns tuples (has a valid tupDesc)
- Uses InvalidOid as table OID since cursors represent query results rather than specific tables
- Complements cursor_to_xml by providing schema-only functionality
- Useful for applications that need to understand result set structure before processing cursor data
- Part of PostgreSQL's cursor-based XML functionality for incremental data processing

## Simplified Source

```c
Datum
cursor_to_xmlschema(PG_FUNCTION_ARGS)
{
    // Extract function parameters
    char *cursor_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    bool include_nulls = PG_GETARG_BOOL(1);
    bool table_forest_format = PG_GETARG_BOOL(2);
    const char *target_namespace = text_to_cstring(PG_GETARG_TEXT_PP(3));

    const char *xmlschema;
    Portal portal;

    SPI_connect();

    // Find and validate the cursor
    portal = SPI_cursor_find(cursor_name);
    if (portal == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_CURSOR),
                       errmsg("cursor \"%s\" does not exist", cursor_name)));
    if (portal->tupDesc == NULL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_STATE),
                       errmsg("portal \"%s\" does not return tuples", cursor_name)));

    // Generate schema from cursor tuple descriptor
    xmlschema = _SPI_strdup(map_sql_table_to_xmlschema(portal->tupDesc,
                                                      InvalidOid, include_nulls,
                                                      table_forest_format, target_namespace));
    SPI_finish();

    PG_RETURN_XML_P(cstring_to_xmltype(xmlschema));
}
```
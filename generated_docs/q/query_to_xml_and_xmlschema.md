# query_to_xml_and_xmlschema

## Location
[src/backend/utils/adt/xml.c:3145-3180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L3145-L3180)

## Overview
Executes a SQL query and returns both the XML representation of the query results and the corresponding XML schema definition.

## Definition

```c
Datum
query_to_xml_and_xmlschema(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a PostgreSQL built-in function that combines the functionality of executing a SQL query and generating both XML data and its schema. It uses the SPI (Server Programming Interface) to prepare and execute the provided query, then generates an XML schema based on the query's tuple descriptor and finally converts the query results to XML format with the embedded schema. The function returns the combined result as an XML type.

The function handles the complete workflow:
1. Connects to SPI and prepares the query
2. Opens a cursor for the query execution
3. Generates the XML schema from the query's tuple descriptor
4. Executes the query and converts results to XML with the schema included
5. Cleans up SPI resources and returns the XML result

## Parameters / Member Variables
- : SQL query string to execute
- : nulls - whether to include NULL values in the XML output
- : tableforest - whether to format output as table forest structure
- : targetns - target namespace for the XML schema

## Dependencies
- Functions called/Symbols referenced:
  - [text_to_cstring](../t/text_to_cstring.md)
  - PG_GETARG_BOOL
  - [SPI_connect](../S/SPI_connect.md)
  - [SPI_prepare](../S/SPI_prepare.md)
  - [SPI_cursor_open](../S/SPI_cursor_open.md)
  - [_SPI_strdup](../S/_SPI_strdup.md)
  - [map_sql_table_to_xmlschema](../m/map_sql_table_to_xmlschema.md)
  - [SPI_cursor_close](../S/SPI_cursor_close.md)
  - [SPI_finish](../S/SPI_finish.md)
  - [stringinfo_to_xmltype](../s/stringinfo_to_xmltype.md)
  - [query_to_xml_internal](query_to_xml_internal.md)
  - PG_RETURN_XML_P
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's function call mechanism)

## Notes and Other Information
- This function is designed to be called from SQL as a built-in function
- Uses SPI for safe query execution within the PostgreSQL backend
- Combines schema generation with data conversion in a single operation
- Located in src/backend/utils/adt/xml.c:3145-3180
- Error handling includes checks for SPI_prepare and SPI_cursor_open failures

## Simplified Source

```c
Datum
query_to_xml_and_xmlschema(PG_FUNCTION_ARGS)
{
    // Extract function parameters
    char *query = text_to_cstring(PG_GETARG_TEXT_PP(0));
    bool nulls = PG_GETARG_BOOL(1);
    bool tableforest = PG_GETARG_BOOL(2);
    const char *targetns = text_to_cstring(PG_GETARG_TEXT_PP(3));

    // Connect to SPI and prepare query
    SPI_connect();
    SPIPlanPtr plan = SPI_prepare(query, 0, NULL);
    if (!plan) elog(ERROR, "SPI_prepare failed");

    // Open cursor for query execution
    Portal portal = SPI_cursor_open(NULL, plan, NULL, NULL, true);
    if (!portal) elog(ERROR, "SPI_cursor_open failed");

    // Generate XML schema from query descriptor
    const char *xmlschema = _SPI_strdup(map_sql_table_to_xmlschema(
        portal->tupDesc, InvalidOid, nulls, tableforest, targetns));

    // Clean up SPI resources
    SPI_cursor_close(portal);
    SPI_finish();

    // Execute query and return XML with embedded schema
    return PG_RETURN_XML_P(stringinfo_to_xmltype(
        query_to_xml_internal(query, NULL, xmlschema, nulls, tableforest, targetns, true)));
}
```
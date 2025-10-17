# cursor_to_xml

## Location
[src/backend/utils/adt/xml.c:2913-2966](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2913-L2966)

## Overview
SQL/XML function that converts data from an existing cursor into XML format, allowing incremental processing of large result sets with controlled row count fetching.

## Definition

```c
Datum
cursor_to_xml(PG_FUNCTION_ARGS)
```
## Detailed Description
The `cursor_to_xml` function provides a cursor-based approach to XML generation, allowing conversion of result sets to XML format while maintaining cursor state for incremental processing. This function is particularly useful for handling large result sets where memory constraints require processing data in chunks rather than loading everything at once.

The function finds an existing cursor by name, fetches a specified number of rows from it, and converts those rows to XML elements. Unlike `query_to_xml` which executes a complete query, this function works with pre-existing cursors that may have been created in a transaction and can be fetched from incrementally. The cursor position is maintained between calls, allowing for pagination-like behavior.

## Parameters / Member Variables
- `name` (text): Name of the existing cursor to fetch data from
- `count` (int32): Number of rows to fetch from the cursor (can be less than available)
- `nulls` (bool): Whether to include NULL values in the XML output
- `tableforest` (bool): Whether to generate XML in table forest format (multiple root elements)
- `targetns` (text): Target XML namespace for the generated XML document

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP
  - [text_to_cstring](../t/text_to_cstring.md)
  - PG_GETARG_INT32
  - PG_GETARG_BOOL
  - [initStringInfo](../i/initStringInfo.md)
  - [xmldata_root_element_start](../x/xmldata_root_element_start.md)
  - [xmldata_root_element_end](../x/xmldata_root_element_end.md)
  - [SPI_connect](../S/SPI_connect.md)
  - [SPI_cursor_find](../S/SPI_cursor_find.md)
  - [SPI_cursor_fetch](../S/SPI_cursor_fetch.md)
  - [SPI_sql_row_to_xmlelement](../S/SPI_sql_row_to_xmlelement.md)
  - [SPI_finish](../S/SPI_finish.md)
  - [stringinfo_to_xmltype](../s/stringinfo_to_xmltype.md)
  - PG_RETURN_XML_P
- Called from (representative examples):
  - No direct callers found (SQL function interface)

## Notes and Other Information
- Designed for incremental processing of large result sets using cursor-based fetching
- Requires an existing cursor to be created beforehand (typically using DECLARE CURSOR)
- Maintains cursor position between calls, enabling pagination through large datasets
- Uses `SPI_cursor_find` to locate the cursor and `SPI_cursor_fetch` to retrieve rows
- Throws ERRCODE_UNDEFINED_CURSOR error if the specified cursor does not exist
- Can fetch fewer rows than requested if cursor reaches end of result set
- More memory-efficient than `query_to_xml` for large result sets
- Part of PostgreSQL's SQL/XML standard compliance features
- Returns XML data type with the fetched rows converted to XML elements
- Uses hardcoded "table" as the root element name (unlike other functions that can derive it)

## Simplified Source

```c
Datum
cursor_to_xml(PG_FUNCTION_ARGS)
{
    // Extract function parameters
    char *cursor_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    int32 row_count = PG_GETARG_INT32(1);
    bool include_nulls = PG_GETARG_BOOL(2);
    bool table_forest_format = PG_GETARG_BOOL(3);
    const char *target_namespace = text_to_cstring(PG_GETARG_TEXT_PP(4));

    StringInfoData result;
    Portal portal;
    uint64 i;

    initStringInfo(&result);

    // Add root element start if not in forest format
    if (!table_forest_format) {
        xmldata_root_element_start(&result, "table", NULL, target_namespace, true);
        appendStringInfoChar(&result, '\n');
    }

    // Connect to SPI and find the cursor
    SPI_connect();
    portal = SPI_cursor_find(cursor_name);
    if (portal == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_CURSOR),
                       errmsg("cursor \"%s\" does not exist", cursor_name)));

    // Fetch rows and convert to XML elements
    SPI_cursor_fetch(portal, true, row_count);
    for (i = 0; i < SPI_processed; i++)
        SPI_sql_row_to_xmlelement(i, &result, NULL, include_nulls,
                                 table_forest_format, target_namespace, true);

    SPI_finish();

    // Add root element end if not in forest format
    if (!table_forest_format)
        xmldata_root_element_end(&result, "table");

    PG_RETURN_XML_P(stringinfo_to_xmltype(&result));
}
```
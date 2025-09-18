# cursor_to_xml

## Location
src/backend/utils/adt/xml.c: 2913 - 2966

## Overview
SQL/XML function that converts data from an existing cursor into XML format, allowing incremental processing of large result sets with controlled row count fetching.

## Definition


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
  - text_to_cstring
  - PG_GETARG_INT32
  - PG_GETARG_BOOL
  - initStringInfo
  - [xmldata_root_element_start](../x/xmldata_root_element_start.md)
  - [xmldata_root_element_end](../x/xmldata_root_element_end.md)
  - SPI_connect
  - [SPI_cursor_find](../S/SPI_cursor_find.md)
  - [SPI_cursor_fetch](../S/SPI_cursor_fetch.md)
  - [SPI_sql_row_to_xmlelement](../S/SPI_sql_row_to_xmlelement.md)
  - SPI_finish
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
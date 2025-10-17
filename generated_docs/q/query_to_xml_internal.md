# query_to_xml_internal

## Location
[src/backend/utils/adt/xml.c:3001-3045](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L3001-L3045)

## Overview
Internal function that converts SQL query results into XML format, providing the core functionality for PostgreSQL's XML generation functions.

## Definition

```c
static StringInfo
query_to_xml_internal(const char *query, char *tablename,
					  const char *xmlschema, bool nulls, bool tableforest,
					  const char *targetns, bool top_level)
```
## Detailed Description
This static function serves as the workhorse for PostgreSQL's XML generation capabilities. It executes a given SQL query using the Server Programming Interface (SPI) and transforms the result set into well-formed XML. The function supports various XML formatting options including schema inclusion, null value handling, and different structural layouts (table vs forest format). It handles the complete XML generation pipeline from query execution to final XML string construction.

## Parameters / Member Variables
- `query`: SQL query string to execute
- `tablename`: Name to use for XML table element (if NULL, defaults to "table")  
- `xmlschema`: Optional XML schema to include in output
- `nulls`: Boolean flag indicating whether to include null values in XML
- `tableforest`: Boolean flag for forest format (multiple root elements) vs single table format
- `targetns`: Target namespace for XML elements
- `top_level`: Boolean indicating if this is a top-level XML document

## Dependencies
- Functions called/Symbols referenced:
  - [map_sql_identifier_to_xml_name](../m/map_sql_identifier_to_xml_name.md)
  - [makeStringInfo](../m/makeStringInfo.md)
  - [SPI_connect](../S/SPI_connect.md)
  - [SPI_execute](../S/SPI_execute.md)
  - SPI_OK_SELECT
  - [xmldata_root_element_start](../x/xmldata_root_element_start.md)
  - [SPI_sql_row_to_xmlelement](../S/SPI_sql_row_to_xmlelement.md)
  - [xmldata_root_element_end](../x/xmldata_root_element_end.md)
  - [SPI_finish](../S/SPI_finish.md)
- Called from:
  - [table_to_xml_internal](../t/table_to_xml_internal.md)
  - [query_to_xml](query_to_xml.md)
  - [query_to_xml_and_xmlschema](query_to_xml_and_xmlschema.md)

## Notes and Other Information
- Function is declared static, making it internal to xml.c
- Uses SPI (Server Programming Interface) for query execution, requiring SPI_connect/SPI_finish pair
- Supports both single-table XML structure and forest format (multiple root elements)
- Automatically maps SQL identifiers to valid XML names
- Error handling includes validation that the query is a SELECT statement
- Memory management handled through StringInfo for dynamic string building

## Simplified Source

```c
static StringInfo
query_to_xml_internal(const char *query, char *tablename,
                     const char *xmlschema, bool nulls, bool tableforest,
                     const char *targetns, bool top_level)
{
    StringInfo result;
    char *xmltn;
    uint64 i;

    // Determine XML table name
    xmltn = tablename ? map_sql_identifier_to_xml_name(tablename, true, false) : "table";

    result = makeStringInfo();

    // Execute the query
    SPI_connect();
    if (SPI_execute(query, true, 0) != SPI_OK_SELECT)
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                       errmsg("invalid query")));

    // Add root element start if not in forest format
    if (!tableforest) {
        xmldata_root_element_start(result, xmltn, xmlschema, targetns, top_level);
        appendStringInfoChar(result, '\n');
    }

    // Include schema if provided
    if (xmlschema)
        appendStringInfo(result, "%s\n\n", xmlschema);

    // Convert each result row to XML element
    for (i = 0; i < SPI_processed; i++)
        SPI_sql_row_to_xmlelement(i, result, tablename, nulls,
                                 tableforest, targetns, top_level);

    // Add root element end if not in forest format
    if (!tableforest)
        xmldata_root_element_end(result, xmltn);

    SPI_finish();
    return result;
}
```
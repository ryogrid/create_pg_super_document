# query_to_xml

## Location
[src/backend/utils/adt/xml.c:2899-2912](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2899-L2912)

## Overview
SQL/XML function that executes an arbitrary SQL query and converts the result set to XML format, providing flexible XML generation from any SELECT query.

## Definition


## Detailed Description
The `query_to_xml` function implements SQL/XML functionality for converting the result of any SQL query into XML format. It serves as a wrapper around `query_to_xml_internal`, extracting function arguments and delegating the XML generation process. The function executes the provided SQL query using the Server Programming Interface (SPI) and transforms each result row into XML elements.

This function provides more flexibility than `table_to_xml` as it can work with any SELECT query, including complex queries with joins, filters, and calculated columns. The XML output format can be customized with options for null handling, table forest format, and XML namespace specification.

## Parameters / Member Variables
- `query` (text): SQL SELECT query to execute and convert to XML
- `nulls` (bool): Whether to include NULL values in the XML output
- `tableforest` (bool): Whether to generate XML in table forest format (multiple root elements)
- `targetns` (text): Target XML namespace for the generated XML document

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP
  - text_to_cstring
  - PG_GETARG_BOOL
  - [query_to_xml_internal](query_to_xml_internal.md)
  - [stringinfo_to_xmltype](../s/stringinfo_to_xmltype.md)
  - PG_RETURN_XML_P
- Called from (representative examples):
  - No direct callers found (SQL function interface)

## Notes and Other Information
- Executes arbitrary SQL queries using SPI (Server Programming Interface)
- More flexible than `table_to_xml` as it can handle complex queries with joins and calculations
- Internally uses `SPI_execute` to run the query and `SPI_sql_row_to_xmlelement` to convert rows
- Supports both single root element and table forest XML formats
- Uses `xmldata_root_element_start` and `xmldata_root_element_end` for proper XML structure
- Part of PostgreSQL's SQL/XML standard compliance features
- Returns XML data type that can be further processed or exported
- [Query](../Q/Query.md) execution is read-only (uses SPI with read-only flag)
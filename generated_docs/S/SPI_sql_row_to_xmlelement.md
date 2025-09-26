# SPI_sql_row_to_xmlelement

## Location
[src/backend/utils/adt/xml.c:4086-4155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L4086-L4155)

## Overview
Maps an SQL row from the active SPI cursor to an XML element representation, following SQL/XML:2008 section 9.10 standards.

## Definition
```c
static void SPI_sql_row_to_xmlelement(uint64 rownum, StringInfo result, char *tablename,
                                     bool nulls, bool tableforest,
                                     const char *targetns, bool top_level)
```

## Detailed Description
This function converts a single row from an SPI result set into XML format. It handles both table forest mode (where each row becomes a separate XML element with the table name) and regular table mode (where rows are wrapped in `<row>` elements). The function processes each column in the row, mapping SQL identifiers to XML-compliant names and converting SQL values to their XML representations. NULL values can optionally be represented using XSI nil attributes.

## Parameters / Member Variables
- `rownum`: The row number in the SPI result set to process
- `result`: StringInfo buffer to append the generated XML content
- `tablename`: Name of the table (used for XML element naming, can be NULL)
- `nulls`: Whether to include NULL values as xsi:nil attributes in the output
- `tableforest`: If true, creates table-named elements; if false, uses `<row>` elements
- `targetns`: Target namespace for the XML elements (can be NULL)
- `top_level`: Whether this is a top-level element (affects namespace handling)

## Dependencies
- Functions called/Symbols referenced:
  - [map_sql_identifier_to_xml_name](../m/map_sql_identifier_to_xml_name.md)
  - [xmldata_root_element_start](../x/xmldata_root_element_start.md)
  - [xmldata_root_element_end](../x/xmldata_root_element_end.md)
  - [SPI_fname](SPI_fname.md)
  - [SPI_getbinval](SPI_getbinval.md)
  - [SPI_gettypeid](SPI_gettypeid.md)
  - [map_sql_value_to_xml_value](../m/map_sql_value_to_xml_value.md)
- Called from (representative examples):
  - [cursor_to_xml](../c/cursor_to_xml.md)
  - [query_to_xml_internal](../q/query_to_xml_internal.md)

## Notes and Other Information
- This is a static function used internally by PostgreSQL's XML processing system
- Relies on the SPI (Server Programming Interface) subsystem being properly initialized
- Handles XML namespace declarations when in tableforest mode and top_level is true
- Column names are automatically mapped to XML-compliant identifiers
- Part of PostgreSQL's SQL/XML standard implementation
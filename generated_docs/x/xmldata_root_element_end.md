# xmldata_root_element_end

## Location
[src/backend/utils/adt/xml.c:2994-3000](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2994-L3000)

## Overview
Internal utility function that generates the closing XML tag for root elements in PostgreSQL's XML data mapping functions, providing the counterpart to xmldata_root_element_start.

## Definition


## Detailed Description
The `xmldata_root_element_end` function is a simple utility function that generates properly formatted XML closing tags for root elements in PostgreSQL's SQL/XML functions. It works in conjunction with `xmldata_root_element_start` to create balanced XML tag pairs that ensure well-formed XML output.

This function is deliberately simple and focuses solely on generating the closing tag with proper formatting, including a newline character for readable XML output. It's called by all major XML generation functions to ensure consistent XML structure and proper tag balancing throughout PostgreSQL's XML functionality.

## Parameters / Member Variables
- `result` (StringInfo): Output buffer where the XML closing tag will be appended
- `eltname` (const char *): Name of the XML element to create the closing tag for (must match the corresponding opening tag)

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfo
- Called from (representative examples):
  - [cursor_to_xml](../c/cursor_to_xml.md)
  - [query_to_xml_internal](../q/query_to_xml_internal.md)  
  - [schema_to_xml_internal](../s/schema_to_xml_internal.md)
  - [database_to_xml_internal](../d/database_to_xml_internal.md)
  - [SPI_sql_row_to_xmlelement](../S/SPI_sql_row_to_xmlelement.md)

## Notes and Other Information
- Internal static function, not exposed as SQL function
- Much simpler than `xmldata_root_element_start` as closing tags don't require namespace or schema attributes
- Ensures proper XML tag balancing across all PostgreSQL XML generation functions
- Adds newline character for readable XML formatting
- Part of PostgreSQL's comprehensive XML generation infrastructure
- Must be called with the same element name as the corresponding `xmldata_root_element_start` call
- Used consistently across all XML-to-data mapping functions for uniform XML structure
- Essential for generating well-formed XML that passes validation
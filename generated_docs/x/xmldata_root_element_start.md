# xmldata_root_element_start

## Location
src/backend/utils/adt/xml.c: 2967 - 2993

## Overview
Internal utility function that generates the opening XML tag for root elements in PostgreSQL's XML data mapping functions, handling namespace declarations and schema location attributes.

## Definition


## Detailed Description
The `xmldata_root_element_start` function is responsible for generating properly formatted XML opening tags for root elements in PostgreSQL's SQL/XML functions. It handles the complex logic of XML namespace declarations, schema location attributes, and ensures that namespace declarations are only included at the appropriate hierarchical level to avoid redundancy and maintain clean XML output.

The function distinguishes between top-level elements (where namespace declarations should be included) and nested elements (where they should be omitted to avoid repetition). It supports both namespace-aware and non-namespace XML generation, and can include XML Schema Instance (XSI) attributes for schema validation when an XML schema is provided.

## Parameters / Member Variables
- `result` (StringInfo): Output buffer where the XML opening tag will be appended
- `eltname` (const char *): Name of the XML element to create the opening tag for
- `xmlschema` (const char *): Optional XML schema reference (NULL if no schema)
- `targetns` (const char *): Target XML namespace URI (can be empty string)
- `top_level` (bool): Whether this is the top-level element (affects namespace declaration inclusion)

## Dependencies
- Functions called/Symbols referenced:
  - Assert
  - appendStringInfo
  - appendStringInfoString
  - strlen
  - NAMESPACE_XSI (constant)
- Called from (representative examples):
  - cursor_to_xml
  - query_to_xml_internal
  - schema_to_xml_internal
  - database_to_xml_internal
  - SPI_sql_row_to_xmlelement

## Notes and Other Information
- Internal static function, not exposed as SQL function
- Handles XML namespace declarations only at top level to avoid ugly repetition in nested elements
- Supports XML Schema Instance (XSI) namespace for schema validation
- Uses schemaLocation attribute when target namespace is specified, noNamespaceSchemaLocation otherwise
- Assert statement prevents inconsistent usage (xmlschema without top_level)
- Part of PostgreSQL's comprehensive XML generation infrastructure
- Works in conjunction with `xmldata_root_element_end` to create balanced XML tags
- Generates namespace-aware XML following W3C standards
- FIXME comment indicates schema location handling could be improved
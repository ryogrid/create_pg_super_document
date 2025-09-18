# xml_is_well_formed_document

## Location
src/backend/utils/adt/xml.c: 4622 - 4634

## Overview
SQL-callable function that checks whether given text data represents a well-formed XML document (not just XML content fragment).

## Definition
```c
Datum xml_is_well_formed_document(PG_FUNCTION_ARGS)
```

## Detailed Description
The `xml_is_well_formed_document` function is a PostgreSQL built-in function that provides XML document well-formedness validation from SQL. Unlike `xml_is_well_formed` which uses the current xmloption setting, this function explicitly validates the input as an XML document by passing `XMLOPTION_DOCUMENT` to the internal `wellformed_xml` function.

This function enforces stricter validation requirements than content validation, requiring the input to be a complete, well-formed XML document with a single root element, proper XML declaration (if present), and adherence to XML document structure rules.

## Parameters / Member Variables
- `PG_GETARG_TEXT_PP(0)`: Text data containing the XML document to validate

## Dependencies
- Functions called/Symbols referenced:
  - wellformed_xml
  - PG_GETARG_TEXT_PP  
  - PG_RETURN_BOOL
  - NO_XML_SUPPORT
  - XMLOPTION_DOCUMENT
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Requires libxml2 support (USE_LIBXML macro must be defined)
- Returns NO_XML_SUPPORT error when libxml2 is not available
- Located in src/backend/utils/adt/xml.c:4622-4634
- Explicitly uses XMLOPTION_DOCUMENT mode for strict document validation
- Part of PostgreSQL's SQL-accessible XML validation functions
- Returns true if input is a well-formed XML document, false otherwise
- Stricter than xml_is_well_formed as it requires complete document structure
- Useful when you need to ensure input conforms to XML document standards
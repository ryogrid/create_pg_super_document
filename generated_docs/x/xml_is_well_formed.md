# xml_is_well_formed

## Location
[src/backend/utils/adt/xml.c:4609-4621](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L4609-L4621)

## Overview
SQL-callable function that checks whether given text data represents well-formed XML using the current xmloption setting.

## Definition
```c
Datum xml_is_well_formed(PG_FUNCTION_ARGS)
```

## Detailed Description
The `xml_is_well_formed` function is a PostgreSQL built-in function that provides XML well-formedness validation from SQL. It serves as a public interface to the internal `wellformed_xml` function, using the global `xmloption` setting to determine how to parse and validate the XML content.

This function takes a text argument containing XML data and returns a boolean indicating whether the XML is well-formed according to XML standards. The validation behavior depends on the current `xmloption` setting, which determines whether to treat the input as an XML document or XML content fragment.

## Parameters / Member Variables
- `PG_GETARG_TEXT_PP(0)`: Text data containing the XML content to validate

## Dependencies
- Functions called/Symbols referenced:
  - [wellformed_xml](../w/wellformed_xml.md)
  - PG_GETARG_TEXT_PP
  - PG_RETURN_BOOL
  - NO_XML_SUPPORT
  - xmloption (global variable)
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Requires libxml2 support (USE_LIBXML macro must be defined)
- Returns NO_XML_SUPPORT error when libxml2 is not available
- Located in src/backend/utils/adt/xml.c:4609-4621
- Uses the global xmloption setting to determine parsing behavior
- Part of PostgreSQL's SQL-accessible XML validation functions
- Returns true if XML is well-formed, false otherwise
- Can be used in SQL queries to validate XML data before processing
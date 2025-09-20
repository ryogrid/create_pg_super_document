# xml_is_well_formed_content

## Location
[src/backend/utils/adt/xml.c:4635-4657](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L4635-L4657)

## Overview
A PostgreSQL function that validates whether a given XML text content is well-formed according to XML standards.

## Definition

```c
Datum
xml_is_well_formed_content(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a SQL-callable interface to validate XML content for well-formedness. It takes a text input containing XML content and returns a boolean indicating whether the XML is properly structured according to XML parsing rules. The function is conditionally compiled based on whether PostgreSQL was built with libxml2 support (USE_LIBXML). When libxml2 support is unavailable, it raises a "no XML support" error.

The function specifically validates XML content (as opposed to XML documents), meaning it can validate XML fragments that don't necessarily have a single root element.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments macro
  - Argument 0:  - The XML content to be validated for well-formedness

## Dependencies
- Functions called/Symbols referenced:
  - [wellformed_xml](../w/wellformed_xml.md) (core validation function)
  - XMLOPTION_CONTENT (specifies content validation mode)
  - NO_XML_SUPPORT (error macro when libxml2 unavailable)
- Called from (representative examples):
  - No direct callers found in the codebase (likely called via SQL interface)

## Notes and Other Information
- This function is only available when PostgreSQL is compiled with libxml2 support (USE_LIBXML macro)
- Returns a boolean value indicating well-formedness
- Part of PostgreSQL's XML data type support system
- Located in src/backend/utils/adt/xml.c:4635-4657
- Validates XML content rather than full XML documents, allowing for XML fragments
# xpath_exists

## Location
src/backend/utils/adt/xml.c: 4567 - 4590

## Overview
Determines if the node specified by the supplied XPath exists in a given XML document, returning a boolean value with namespace support.

## Definition
```c
Datum xpath_exists(PG_FUNCTION_ARGS)
```

## Detailed Description
The `xpath_exists` function is similar to `xmlexists` but provides enhanced functionality by supporting XML namespaces. This function evaluates an XPath expression against an XML document and returns a boolean indicating whether any matching nodes exist. Unlike `xmlexists`, this function is not defined in the SQL/XML standard but provides PostgreSQL-specific namespace handling capabilities.

The function accepts an additional namespaces parameter (as an array) that allows for proper XPath evaluation in XML documents that use namespaces. It internally uses the `xpath_internal` function to perform the XPath evaluation and checks if any results were returned to determine the boolean result.

## Parameters / Member Variables
- `PG_GETARG_TEXT_PP(0)`: XPath expression as text - the XPath query to evaluate against the XML document
- `PG_GETARG_XML_P(1)`: XML data - the XML document to search within  
- `PG_GETARG_ARRAYTYPE_P(2)`: Namespaces array - array of namespace declarations for XPath evaluation

## Dependencies
- Functions called/Symbols referenced:
  - xpath_internal
  - PG_GETARG_TEXT_PP
  - PG_GETARG_XML_P
  - PG_GETARG_ARRAYTYPE_P
  - PG_RETURN_BOOL
  - NO_XML_SUPPORT
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Requires libxml2 support (USE_LIBXML macro must be defined)
- Returns NO_XML_SUPPORT error when libxml2 is not available
- Located in src/backend/utils/adt/xml.c:4567-4590
- Differs from xmlexists by supporting namespaces and not being defined in SQL/XML standard
- Returns true if one or more nodes match the XPath expression, false otherwise
- Part of PostgreSQL's extended XML processing capabilities
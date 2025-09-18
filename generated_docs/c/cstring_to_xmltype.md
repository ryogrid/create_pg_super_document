# cstring_to_xmltype

## Location
src/backend/utils/adt/xml.c: 474 - 481

## Overview
Converts a null-terminated C string containing XML data into a PostgreSQL xmltype value.

## Definition


## Detailed Description
The cstring_to_xmltype function is a simple utility that converts a standard null-terminated C string into a PostgreSQL xmltype value. This function provides a convenient interface for creating XML values from C string literals or other null-terminated string sources.

The function works by delegating to cstring_to_text, which handles the conversion from C string to PostgreSQL's internal text representation. Since xmltype is internally represented as text in PostgreSQL, a simple cast is sufficient to complete the conversion. The function automatically calculates the string length using strlen internally within cstring_to_text.

## Parameters / Member Variables
- `string`: Null-terminated C string containing XML data to be converted

## Dependencies
- Functions called/Symbols referenced:
  - cstring_to_text (converts C string to PostgreSQL text)
  - [xmltype](../x/xmltype.md) (PostgreSQL XML data type)
- Called from:
  - [table_to_xmlschema](../t/table_to_xmlschema.md) (table to XML schema conversion)
  - [query_to_xmlschema](../q/query_to_xmlschema.md) (query to XML schema conversion)
  - [cursor_to_xmlschema](cursor_to_xmlschema.md) (cursor to XML schema conversion)
  - [xml_xpathobjtoxmlarray](../x/xml_xpathobjtoxmlarray.md) (XPath object to XML array conversion)

## Notes and Other Information
- This is a static function, only available within the xml.c compilation unit
- The function assumes the input string contains valid XML data
- No validation of XML content is performed by this function
- The input string must be null-terminated (unlike stringinfo_to_xmltype which works with explicit lengths)
- Primarily used when converting pre-built XML strings, especially XML schema definitions
- Less commonly used than stringinfo_to_xmltype since most XML generation uses StringInfo buffers
- The function handles memory allocation automatically through cstring_to_text
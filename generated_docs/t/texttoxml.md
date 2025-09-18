# texttoxml

## Location
src/backend/utils/adt/xml.c: 637 - 645

## Overview
Converts a text value containing XML content into PostgreSQL's XML data type by parsing the input text.

## Definition
```c
Datum texttoxml(PG_FUNCTION_ARGS)
```

## Detailed Description
The texttoxml function provides a simple interface to convert text input into PostgreSQL's XML data type. It takes a text parameter containing XML content and uses the xmlparse function to convert it into a properly parsed and validated XML value.

The function internally uses the global xmloption setting and enables validation (third parameter set to true) when parsing the XML content. This ensures that the input text contains well-formed XML before converting it to the XML type.

This function serves as a bridge between PostgreSQL's text and XML data types, allowing users to convert textual representations of XML into the proper XML type for further XML operations.

## Parameters / Member Variables
- `data`: Text input parameter (accessed via `PG_GETARG_TEXT_PP(0)`) containing XML content to be parsed

## Dependencies
- Functions called/Symbols referenced:
  - [xmlparse](../x/xmlparse.md)
  - PG_RETURN_XML_P
- Called from (representative examples):
  - No direct references found in codebase (likely used via SQL interface)

## Notes and Other Information
- Uses the global xmloption setting to determine parsing behavior
- Enables validation during XML parsing (third parameter to xmlparse is true)
- Provides a simple conversion path from text to XML data type
- Part of PostgreSQL's XML support infrastructure
- Validation ensures input contains well-formed XML before type conversion
- Follows PostgreSQL's function calling convention with PG_FUNCTION_ARGS
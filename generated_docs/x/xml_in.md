# xml_in

## Location
[src/backend/utils/adt/xml.c:273-300](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L273-L300)

## Overview
PostgreSQL input function that converts a C string representation of XML data into the internal xml data type, with validation for well-formed XML.

## Definition

```c
Datum
xml_in(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the input conversion function for PostgreSQL's xml data type. It takes a C string containing XML data and converts it into PostgreSQL's internal xml representation. The function performs well-formedness validation by parsing the XML data using libxml2. The conversion process reuses the text data type's conversion mechanism since xml and text types share the same internal representation. The function is conditionally compiled and only available when PostgreSQL is built with libxml2 support (USE_LIBXML).

## Parameters / Member Variables
- Takes PostgreSQL function arguments via  macro:
  - Argument 0: C string containing the XML data to be converted

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING
  - [cstring_to_text](../c/cstring_to_text.md)
  - [xml_parse](xml_parse.md)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - xmlFreeDoc
  - PG_RETURN_XML_P
  - NO_XML_SUPPORT (fallback when libxml2 not available)
- Called from (representative examples):
  - PostgreSQL's type input/output system (indirectly via function registry)

## Notes and Other Information
- This is a PostgreSQL I/O function, typically registered in the system catalogs as the input function for the xml type
- The function performs XML well-formedness validation during input, ensuring that only valid XML is stored
- Relies on shared representation between xml and text types for efficiency
- When USE_LIBXML is not defined, calls NO_XML_SUPPORT() macro which reports an error
- Memory management: temporarily creates an xmlDocPtr for validation but frees it immediately after parsing
- Uses the current database encoding when parsing XML data
- The soft error handling allows for graceful error management in the parsing context
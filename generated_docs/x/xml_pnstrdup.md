# xml_pnstrdup

## Location
[src/backend/utils/adt/xml.c:1375-1386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L1375-L1386)

## Overview
Creates a null-terminated copy of an xmlChar string with a specified length, similar to PostgreSQL's pnstrdup but for xmlChar data.

## Definition
```c
static xmlChar *xml_pnstrdup(const xmlChar *str, size_t len)
```

## Detailed Description
The xml_pnstrdup function is a specialized string duplication utility designed specifically for xmlChar data types used by libxml. It functions similarly to PostgreSQL's pnstrdup function but operates on xmlChar arrays rather than regular char arrays.

The function allocates memory for a new xmlChar array, copies the specified number of characters from the source string, and ensures the result is null-terminated. The length parameter is measured in xmlChar units, not bytes, which is important for proper handling of XML character data that may use different character encodings.

This function is used internally within PostgreSQL's XML processing code to create copies of XML string data with controlled lengths, particularly useful when parsing XML declarations and other structured XML content.

## Parameters / Member Variables
- `str`: Pointer to the source xmlChar array to be duplicated
- `len`: Number of xmlChar elements to copy from the source string

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
  - memcpy (standard C memory copy function)

- Called from (representative examples):
  - [parse_xml_decl](../p/parse_xml_decl.md) (multiple calls for parsing XML declarations)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the xml.c file
- The function handles xmlChar data specifically, which may differ from regular char in size and encoding
- Memory is allocated using PostgreSQL's palloc, ensuring proper integration with PostgreSQL's memory management
- The length parameter is measured in xmlChar units, not bytes
- The function always null-terminates the result, making it safe for use with string functions expecting null-terminated strings
- Used primarily for parsing XML declaration components where specific string lengths need to be extracted
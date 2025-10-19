# pg_xmlCharStrndup

## Location
[src/backend/utils/adt/xml.c:1387-1403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L1387-L1403)

## Overview
Converts a regular C string to a null-terminated xmlChar string with a specified length, enabling conversion from char* to xmlChar* for libxml compatibility.

## Definition
```c
static xmlChar *pg_xmlCharStrndup(const char *str, size_t len)
```

## Detailed Description
The pg_xmlCharStrndup function is a utility that converts regular C strings (char*) to xmlChar strings used by libxml. This function is essential for interfacing between PostgreSQL's standard string handling and libxml's xmlChar-based APIs.

The function allocates memory for a new xmlChar array, copies the specified number of bytes from the source char string, and null-terminates the result. Unlike xml_pnstrdup which works with xmlChar input, this function accepts regular char* input, making it useful for converting PostgreSQL text data to xmlChar format for use with libxml functions.

The function is widely used throughout PostgreSQL's XML processing code, particularly in XPath operations and XML table functions where PostgreSQL string data needs to be passed to libxml APIs.

## Parameters / Member Variables
- `str`: Pointer to the source char array (regular C string) to be converted
- `len`: Number of bytes to copy from the source string

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](palloc.md) (PostgreSQL memory allocation function)
  - memcpy (standard C memory copy function)

- Called from (representative examples):
  - [PgXmlErrorContext](../P/PgXmlErrorContext.md) (structure initialization)
  - [xpath_internal](../x/xpath_internal.md) (for XPath expression processing)
  - [XmlTableSetDocument](../X/XmlTableSetDocument.md) (XML table document setup)
  - [XmlTableSetNamespace](../X/XmlTableSetNamespace.md) (XML namespace handling)
  - [XmlTableSetRowFilter](../X/XmlTableSetRowFilter.md) (XML row filtering)
  - [XmlTableSetColumnFilter](../X/XmlTableSetColumnFilter.md) (XML column filtering)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the xml.c file
- The function performs type conversion from char* to xmlChar*, which may involve encoding considerations
- Memory is allocated using PostgreSQL's palloc for proper memory management integration
- The length parameter is measured in bytes, not characters, which is important for multi-byte encodings
- The function always null-terminates the result, ensuring compatibility with string functions
- Widely used in XML table functions and XPath processing where PostgreSQL strings need to be converted to libxml format
- The function assumes that xmlChar and char have compatible byte representations for the copying operation

## Simplified Source

```c
static xmlChar *
pg_xmlCharStrndup(const char *str, size_t len)
{
    // Allocate memory for xmlChar array (including null terminator)
    xmlChar *result = (xmlChar *) palloc((len + 1) * sizeof(xmlChar));

    // Copy specified number of bytes from source string
    memcpy(result, str, len);

    // Null-terminate the result
    result[len] = '\0';

    return result;
}
```
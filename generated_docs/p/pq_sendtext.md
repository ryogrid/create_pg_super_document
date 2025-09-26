# pq_sendtext

## Location
[src/backend/libpq/pqformat.c:172-194](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqformat.c#L172-L194)

## Overview
Appends a text string with character set conversion to a StringInfo buffer without length prefixing, primarily used for binary format conversions.

## Definition

```c
void
pq_sendtext(StringInfo buf, const char *str, int slen)
```
## Detailed Description
This function appends a text string to a StringInfo buffer after performing character set conversion from server encoding to client encoding. Unlike , it does not include a length prefix, making it unsuitable for direct frontend transmissions where the receiver needs to know the string length. Instead, it's primarily designed for binary format conversions where the length information is handled elsewhere in the protocol.

The function handles character set conversion intelligently by checking if  actually performed a conversion. If conversion occurred (indicated by a different pointer), it recalculates the string length, appends the converted data, and frees the temporary converted string. If no conversion was needed, it directly appends the original string.

## Parameters / Member Variables
- : StringInfo buffer to append the text to
- : Pointer to the input text string (need not be null-terminated)
- : Length of the input string in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [pg_server_to_client](pg_server_to_client.md) (character set conversion)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md) (append binary data with trailing null)
  - strlen (get length of converted string)
  - [pfree](pfree.md) (free converted string memory)
- Called from (representative examples):
  - [enum_send](../e/enum_send.md)
  - [json_send](../j/json_send.md)
  - [jsonb_send](../j/jsonb_send.md)
  - [jsonpath_send](../j/jsonpath_send.md)
  - [namesend](../n/namesend.md)
  - [cstring_send](../c/cstring_send.md)
  - [tsvectorsend](../t/tsvectorsend.md)
  - [textsend](../t/textsend.md)
  - [unknownsend](../u/unknownsend.md)
  - [xml_send](../x/xml_send.md)

## Notes and Other Information
- Does not include a length prefix, making it unsuitable for direct frontend communication where length is needed
- Primarily intended for binary format conversions where length is managed separately
- Input string does not need to be null-terminated, and output maintains the same characteristic
- Automatically handles character set conversion and memory management for converted strings
- Uses appendBinaryStringInfo (which maintains a trailing null-byte) unlike pq_sendcountedtext which uses appendBinaryStringInfoNT
- Commonly used by data type send functions that need to transmit textual data in binary format
- Part of PostgreSQL's type system for converting internal representations to wire format
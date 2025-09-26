# pq_sendcountedtext

## Location
src/backend/libpq/pqformat.c: 142 - 171

## Overview
Appends a counted text string with character set conversion to a StringInfo buffer, formatting it according to PostgreSQL protocol version 3.0 requirements.

## Definition

```c
void
pq_sendcountedtext(StringInfo buf, const char *str, int slen)
```
## Detailed Description
This function formats and appends a text string to a StringInfo buffer according to PostgreSQL's wire protocol requirements. It sends a 4-byte length field followed by the string data, where the length count does not include itself as mandated by protocol version 3.0. The function also handles character set conversion from the server's encoding to the client's encoding using .

The function intelligently handles character set conversion by checking if actual conversion occurred. If conversion was performed (indicated by a different pointer being returned), it recalculates the string length and frees the converted string. If no conversion was needed, it uses the original string directly. The input string does not need to be null-terminated, and the transmitted data is also not null-terminated.

## Parameters / Member Variables
- : StringInfo buffer to append the counted text to
- : Pointer to the text string to send (need not be null-terminated)
- : Length of the input string in bytes

## Dependencies
- Functions called/Symbols referenced:
  - pg_server_to_client (character set conversion)
  - pq_sendint32 (send 4-byte length prefix)
  - appendBinaryStringInfoNT (append binary data without trailing null)
  - strlen (get length of converted string)
  - pfree (free converted string memory)
- Called from (representative examples):
  - printsimple
  - printtup
  - serializeAnalyzeReceive
  - logicalrep_write_tuple
  - SendFunctionResult

## Notes and Other Information
- Implements PostgreSQL protocol version 3.0 counted string format (4-byte length + data)
- Automatically handles character set conversion between server and client encodings
- The length field excludes itself from the count, as required by the protocol specification
- Input string does not need to be null-terminated, and output is not null-terminated either
- Memory management is handled automatically - converted strings are freed after use
- Uses appendBinaryStringInfoNT to avoid adding a trailing null byte to the protocol data
- Critical for sending text data that needs to be properly encoded for the client's character set
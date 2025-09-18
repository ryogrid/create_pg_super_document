# pq_sendstring

## Location
src/backend/libpq/pqformat.c: 195 - 226

## Overview
Appends a null-terminated text string to a StringInfo buffer with character set conversion from server to client encoding.

## Definition


## Detailed Description
The pq_sendstring function is responsible for appending a null-terminated string to a StringInfo buffer while performing character encoding conversion from server encoding to client encoding. This is essential for PostgreSQL's client-server communication protocol where strings need to be converted to the appropriate character set before being sent to the client. The function automatically handles the conversion process and ensures the output string remains null-terminated.

The function first determines the length of the input string, then calls pg_server_to_client() to perform the necessary character set conversion. If conversion actually occurs (indicated by the returned pointer being different from the input), it uses the converted string and frees the temporary conversion buffer. Otherwise, it uses the original string directly. In both cases, it appends the string including its null terminator to the buffer.

## Parameters / Member Variables
- : StringInfo buffer to append the converted string to
- : Null-terminated input string to be converted and appended

## Dependencies
- Functions called/Symbols referenced:
  - [pg_server_to_client](pg_server_to_client.md) (performs character set conversion)
  - appendBinaryStringInfoNT (appends binary data including null terminator)
  - strlen (calculates string length)
  - [pfree](pfree.md) (frees converted string if conversion occurred)

- Called from (representative examples):
  - [NotifyMyFrontEnd](../N/NotifyMyFrontEnd.md) (async notification system)
  - logicalrep_write_* functions (logical replication protocol)
  - [ReportGUCOption](../R/ReportGUCOption.md) (GUC parameter reporting)
  - [SendNegotiateProtocolVersion](../S/SendNegotiateProtocolVersion.md) (protocol negotiation)

## Notes and Other Information
- The input string must be null-terminated as documented
- The function ensures the output is also null-terminated by adding 1 to the string length
- Character set conversion is conditional - if no conversion is needed, the original string is used directly
- This function is part of PostgreSQL's libpq format utilities used in client-server communication
- Memory management is handled automatically - converted strings are properly freed when no longer needed
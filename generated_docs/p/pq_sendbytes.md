# pq_sendbytes

## Location
[src/backend/libpq/pqformat.c:126-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqformat.c#L126-L141)

## Overview
Appends raw binary data to a StringInfo buffer as part of PostgreSQL protocol message construction.

## Definition

```c
void
pq_sendbytes(StringInfo buf, const void *data, int datalen)
```
## Detailed Description
This function is a core component of PostgreSQL's message formatting system that appends raw binary data to a StringInfo buffer. It serves as a wrapper around , specifically choosing the variant that maintains a trailing null-byte for added safety. The function is used extensively throughout PostgreSQL's backend to add binary data to protocol messages, including serialized data structures, authentication tokens, and various data types that need to be transmitted in their binary form.

The function is designed to handle any type of binary data and is commonly used for appending serialized aggregate states, array data, range types, and other complex data structures to outgoing messages.

## Parameters / Member Variables
- : StringInfo buffer to append the data to (typically initialized with pq_beginmessage or pq_beginmessage_reuse)
- : Pointer to the raw binary data to be appended
- : Length of the data in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md) (from StringInfo API)
- Called from (representative examples):
  - [printtup](printtup.md)
  - [serializeAnalyzeReceive](../s/serializeAnalyzeReceive.md)
  - [sendAuthRequest](../s/sendAuthRequest.md)
  - [logicalrep_write_message](../l/logicalrep_write_message.md)
  - [logicalrep_write_tuple](../l/logicalrep_write_tuple.md)
  - [SendTimeLineHistory](../S/SendTimeLineHistory.md)
  - [SendFunctionResult](../S/SendFunctionResult.md)
  - [array_agg_serialize](../a/array_agg_serialize.md)
  - [array_send](../a/array_send.md)
  - [multirange_send](../m/multirange_send.md)
  - [range_send](../r/range_send.md)
  - [record_send](../r/record_send.md)
  - [uuid_send](../u/uuid_send.md)
  - [varbit_send](../v/varbit_send.md)
  - [string_agg_serialize](../s/string_agg_serialize.md)

## Notes and Other Information
- Uses the safe variant of appendBinaryStringInfo that maintains a trailing null-byte for safety
- Handles arbitrary binary data, making it suitable for complex data types and serialized structures
- Commonly used in conjunction with other pq_sendXXX functions to build complete protocol messages
- Part of the family of pq_sendXXX functions that provide type-specific and general-purpose data appending capabilities
- The trailing null-byte maintained by the underlying appendBinaryStringInfo provides additional safety margins
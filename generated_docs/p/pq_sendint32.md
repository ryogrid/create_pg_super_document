# pq_sendint32

## Location
[src/include/libpq/pqformat.h:144-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/pqformat.h#L144-L151)

## Overview
Appends a binary 32-bit unsigned integer to a StringInfo buffer for network transmission in PostgreSQL protocol messages.

## Definition
```c
static inline void pq_sendint32(StringInfo buf, uint32 i)
```

## Detailed Description
`pq_sendint32` is an inline function that appends a 32-bit unsigned integer to a StringInfo buffer. This is one of the most frequently used functions in PostgreSQL's protocol layer, appearing throughout logical replication, data type serialization, authentication, and general protocol communication. The function ensures proper buffer space allocation through `enlargeStringInfo` before writing the integer in network byte order using `pq_writeint32`. Its 32-bit capacity makes it ideal for representing object IDs, transaction IDs, array dimensions, message lengths, and most numeric protocol fields.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the 32-bit integer will be appended
- `i`: The 32-bit unsigned integer value to append to the buffer

## Dependencies
- Functions called/Symbols referenced:
  - [enlargeStringInfo](../e/enlargeStringInfo.md)
  - [pq_writeint32](pq_writeint32.md)
- Called from (representative examples):
  - [printsimple_startup](printsimple_startup.md)
  - [printtup](printtup.md)
  - [sendAuthRequest](../s/sendAuthRequest.md)
  - [pq_sendcountedtext](pq_sendcountedtext.md)
  - [logicalrep_write_begin](../l/logicalrep_write_begin.md)
  - [logicalrep_write_insert](../l/logicalrep_write_insert.md)
  - [logicalrep_write_update](../l/logicalrep_write_update.md)
  - [array_send](../a/array_send.md)
  - [int4send](../i/int4send.md)
  - [numeric_serialize](../n/numeric_serialize.md)
  - [record_send](../r/record_send.md)
  - [pq_sendint](pq_sendint.md)

## Notes and Other Information
- Defined as a static inline function for optimal performance
- Most heavily used integer sending function in PostgreSQL protocol
- Critical for logical replication message formatting
- Used extensively in data type serialization functions
- Handles network byte order conversion automatically
- Essential for PostgreSQL's wire protocol compatibility
- The 32-bit size accommodates most PostgreSQL internal identifiers and counters
- Used in both client-server and replication protocols
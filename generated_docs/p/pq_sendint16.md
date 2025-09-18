# pq_sendint16

## Location
[src/include/libpq/pqformat.h:136-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/pqformat.h#L136-L143)

## Overview
Appends a binary 16-bit unsigned integer to a StringInfo buffer for network transmission in PostgreSQL protocol messages.

## Definition
```c
static inline void pq_sendint16(StringInfo buf, uint16 i)
```

## Detailed Description
`pq_sendint16` is an inline function that efficiently appends a 16-bit unsigned integer to a StringInfo buffer. This function is widely used throughout PostgreSQL for constructing binary protocol messages, particularly in row description messages, copy operations, and logical replication. It follows the same pattern as other pq_send functions by first ensuring adequate buffer space through `enlargeStringInfo`, then writing the integer in network byte order using `pq_writeint16`. The function is critical for maintaining protocol compatibility and ensuring consistent data transmission across different platforms.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the 16-bit integer will be appended
- `i`: The 16-bit unsigned integer value to append to the buffer

## Dependencies
- Functions called/Symbols referenced:
  - enlargeStringInfo
  - [pq_writeint16](pq_writeint16.md)
- Called from (representative examples):
  - [printsimple_startup](printsimple_startup.md)
  - [SendRowDescriptionMessage](../S/SendRowDescriptionMessage.md)
  - [printtup](printtup.md)
  - [SendCopyBegin](../S/SendCopyBegin.md)
  - [ReceiveCopyBegin](../R/ReceiveCopyBegin.md)
  - [logicalrep_write_tuple](../l/logicalrep_write_tuple.md)
  - [int2send](../i/int2send.md)
  - [numeric_send](../n/numeric_send.md)
  - [tsvectorsend](../t/tsvectorsend.md)
  - [pq_sendint](pq_sendint.md)

## Notes and Other Information
- Defined as a static inline function for optimal performance
- Extensively used in PostgreSQL protocol message construction
- Critical for row descriptions, copy operations, and type serialization
- Handles network byte order conversion automatically
- Used in both frontend and backend protocol communications
- The 16-bit size makes it suitable for counts, lengths, and small numeric values in protocol messages
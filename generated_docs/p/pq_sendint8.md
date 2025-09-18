# pq_sendint8

## Location
src/include/libpq/pqformat.h: 128 - 135

## Overview
Appends a binary 8-bit unsigned integer to a StringInfo buffer for network transmission in PostgreSQL protocol messages.

## Definition
```c
static inline void pq_sendint8(StringInfo buf, uint8 i)
```

## Detailed Description
`pq_sendint8` is an inline function that efficiently appends an 8-bit unsigned integer to a StringInfo buffer. This function is part of PostgreSQL's protocol formatting utilities used for constructing binary protocol messages. It first ensures the buffer has sufficient space by calling `enlargeStringInfo`, then writes the integer using `pq_writeint8`. Being defined as a static inline function, it provides optimal performance for frequent operations while maintaining type safety for 8-bit integer operations.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the 8-bit integer will be appended
- `i`: The 8-bit unsigned integer value to append to the buffer

## Dependencies
- Functions called/Symbols referenced:
  - enlargeStringInfo
  - [pq_writeint8](pq_writeint8.md)
- Called from (representative examples):
  - [logicalrep_write_truncate](../l/logicalrep_write_truncate.md)
  - [logicalrep_write_message](../l/logicalrep_write_message.md)
  - [jsonb_send](../j/jsonb_send.md)
  - [jsonpath_send](../j/jsonpath_send.md)
  - [tsquerysend](../t/tsquerysend.md)
  - [pq_sendbyte](pq_sendbyte.md)
  - [pq_sendint](pq_sendint.md)

## Notes and Other Information
- Defined as a static inline function for optimal performance
- Part of the PostgreSQL protocol formatting API
- Used extensively in logical replication and data type serialization
- Automatically handles buffer expansion to accommodate the new data
- The function is located in the header file for inline expansion at compile time
# pq_writeint16

## Location
[src/include/libpq/pqformat.h:60-73](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/pqformat.h#L60-L73)

## Overview
A static inline function that appends a 16-bit unsigned integer to a StringInfo buffer in network byte order for PostgreSQL's libpq protocol format handling.

## Definition
```c
static inline void pq_writeint16(StringInfoData *pg_restrict buf, uint16 i)
```

## Detailed Description
The `pq_writeint16` function is a binary protocol serialization utility that writes a 16-bit unsigned integer value to a pre-allocated StringInfo buffer. Unlike `pq_writeint8`, this function performs byte order conversion using `pg_hton16` to ensure the value is stored in network byte order (big-endian), which is the standard format used in PostgreSQL's wire protocol.

The function is implemented as a static inline function for optimal performance in protocol message construction. It uses the `pg_restrict` qualifier to enable compiler optimizations and includes an assertion to verify sufficient buffer space before writing.

## Parameters / Member Variables
- `buf`: A pointer to a StringInfoData structure representing the output buffer. Must have sufficient pre-allocated space for the 16-bit value.
- `i`: The 16-bit unsigned integer value to be written to the buffer in host byte order (will be converted to network byte order).

## Dependencies
- Functions called/Symbols referenced:
  - pg_hton16 (byte order conversion function)
  - Assert (macro)
  - memcpy (standard library function)
- Called from (representative examples):
  - [SendRowDescriptionMessage](../S/SendRowDescriptionMessage.md)
  - [pq_sendint16](pq_sendint16.md)

## Notes and Other Information
- Automatically converts from host byte order to network byte order using pg_hton16
- The function assumes the buffer has been pre-allocated with sufficient space and will assert-fail in debug builds if this precondition is violated
- Uses `pg_restrict` annotations for performance optimization
- Critical component of PostgreSQL's binary protocol used for row description messages and other protocol communications
- Part of the family of pq_writeint functions that handle different integer sizes with appropriate endianness handling

## Simplified Source

```c
// Simplified version of pq_writeint16
static inline void pq_writeint16(StringInfoData *buf, uint16 i) {
    // Convert host byte order to network byte order
    uint16 network_order_value = pg_hton16(i);

    // Verify buffer has enough space (debug builds only)
    Assert(buf->len + sizeof(uint16) <= buf->maxlen);

    // Copy the network-ordered bytes to buffer
    memcpy(buf->data + buf->len, &network_order_value, sizeof(uint16));

    // Update buffer length
    buf->len += sizeof(uint16);
}
```

Key simplifications made:
- Added descriptive variable name for clarity
- Added comments explaining each step
- Removed pg_restrict qualifiers for simplicity
- Focused on the core logic: convert byte order, verify space, copy data, update length
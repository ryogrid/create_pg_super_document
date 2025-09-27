# pq_writeint32

## Location
[src/include/libpq/pqformat.h:74-87](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/pqformat.h#L74-L87)

## Overview
A static inline function that appends a 32-bit unsigned integer to a StringInfo buffer in network byte order for PostgreSQL's libpq protocol format handling.

## Definition
```c
static inline void pq_writeint32(StringInfoData *pg_restrict buf, uint32 i)
```

## Detailed Description
The `pq_writeint32` function is a binary protocol serialization utility that writes a 32-bit unsigned integer value to a pre-allocated StringInfo buffer. The function performs byte order conversion using `pg_hton32` to ensure the value is stored in network byte order (big-endian), which is the standard format used in PostgreSQL's wire protocol for all multi-byte integer values.

This function is commonly used throughout PostgreSQL's protocol handling for writing length fields, object identifiers, and other 32-bit numeric values in protocol messages. It is implemented as a static inline function for optimal performance and uses memory restriction annotations for compiler optimization.

## Parameters / Member Variables
- `buf`: A pointer to a StringInfoData structure representing the output buffer. Must have sufficient pre-allocated space for the 32-bit value.
- `i`: The 32-bit unsigned integer value to be written to the buffer in host byte order (will be converted to network byte order).

## Dependencies
- Functions called/Symbols referenced:
  - pg_hton32 (byte order conversion function)
  - Assert (macro)
  - memcpy (standard library function)
- Called from (representative examples):
  - [SendRowDescriptionMessage](../S/SendRowDescriptionMessage.md)
  - [pq_sendint32](pq_sendint32.md)

## Notes and Other Information
- Automatically converts from host byte order to network byte order using pg_hton32
- The function assumes the buffer has been pre-allocated with sufficient space and will assert-fail in debug builds if this precondition is violated
- Uses `pg_restrict` annotations for performance optimization by informing the compiler about non-overlapping memory regions
- Extensively used in PostgreSQL's protocol implementation for transmitting row description metadata, message lengths, and object identifiers
- Part of the core protocol serialization infrastructure that ensures consistent data representation across different machine architectures

## Simplified Source

```c
// Simplified version of pq_writeint32
static inline void
pq_writeint32(StringInfoData *buf, uint32 i)
{
    // Convert to network byte order (big-endian)
    uint32 network_value = pg_hton32(i);

    // Verify buffer has sufficient space
    Assert(buf->len + (int) sizeof(uint32) <= buf->maxlen);

    // Copy 32-bit value to buffer
    memcpy(buf->data + buf->len, &network_value, sizeof(uint32));

    // Update buffer length
    buf->len += sizeof(uint32);
}
```

Key simplifications made:
- Renamed variable from `ni` to `network_value` for clarity
- Added explanatory comments for each step
- Removed `pg_restrict` annotations for readability
- Preserved essential logic: convert byte order, validate space, copy data, update length
- Maintained the critical network byte order conversion for protocol compatibility
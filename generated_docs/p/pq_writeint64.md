# pq_writeint64

## Location
[src/include/libpq/pqformat.h:88-107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/pqformat.h#L88-L107)

## Overview
A static inline function that appends a 64-bit unsigned integer to a StringInfo buffer in network byte order for PostgreSQL's libpq protocol format handling.

## Definition
```c
static inline void pq_writeint64(StringInfoData *pg_restrict buf, uint64 i)
```

## Detailed Description
The `pq_writeint64` function is a binary protocol serialization utility that writes a 64-bit unsigned integer value to a pre-allocated StringInfo buffer. The function performs byte order conversion using `pg_hton64` to ensure the value is stored in network byte order (big-endian), maintaining consistency with PostgreSQL's wire protocol standards for all multi-byte values.

This function is used for serializing large numeric values, timestamps, large object identifiers, and other 64-bit data types that need to be transmitted over PostgreSQL's binary protocol. Like its smaller counterparts, it is implemented as a static inline function for maximum performance in protocol message construction.

## Parameters / Member Variables
- `buf`: A pointer to a StringInfoData structure representing the output buffer. Must have sufficient pre-allocated space for the 64-bit value.
- `i`: The 64-bit unsigned integer value to be written to the buffer in host byte order (will be converted to network byte order).

## Dependencies
- Functions called/Symbols referenced:
  - pg_hton64 (byte order conversion function)
  - Assert (macro)
  - memcpy (standard library function)
- Called from (representative examples):
  - [pq_sendint64](pq_sendint64.md)

## Notes and Other Information
- Automatically converts from host byte order to network byte order using pg_hton64
- The function assumes the buffer has been pre-allocated with sufficient space and will assert-fail in debug builds if this precondition is violated
- Uses `pg_restrict` annotations for performance optimization
- Essential for handling large numeric values, timestamps, and other 64-bit data types in PostgreSQL's binary protocol
- Completes the family of pq_writeint functions that provide consistent endianness handling across all integer sizes (8, 16, 32, and 64 bits)
- Critical for cross-platform compatibility as it ensures consistent data representation regardless of the host machine's native byte order

## Simplified Source

```c
// Simplified version of pq_writeint64
static inline void pq_writeint64(StringInfoData *pg_restrict buf, uint64 i) {
    // Convert to network byte order (big-endian)
    uint64 ni = pg_hton64(i);

    // Verify buffer has sufficient pre-allocated space
    Assert(buf->len + (int) sizeof(uint64) <= buf->maxlen);

    // Copy 64-bit value to buffer
    memcpy((char *pg_restrict) (buf->data + buf->len), &ni, sizeof(uint64));

    // Update buffer length
    buf->len += sizeof(uint64);
}
```

Key simplifications made:
- Added clear comments explaining byte order conversion and buffer operations
- Preserved the essential endianness handling for network protocol
- Maintained the buffer bounds checking assertion
- Function is already quite efficient, minimal changes needed
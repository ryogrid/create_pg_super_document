# lo_ntoh64

## Location
[src/interfaces/libpq/fe-lobj.c:1048-1064](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-lobj.c#L1048-L1064)

## Overview
Static utility function that converts a 64-bit integer from network byte order to host byte order for large object network protocol communications.

## Definition
```c
static pg_int64 lo_ntoh64(pg_int64 net64)
```

## Detailed Description
This static function performs byte order conversion for 64-bit integers, converting from network byte order (big-endian) to host byte order. It implements the conversion by splitting the network-ordered 64-bit value into two 32-bit halves, converting each half using the existing pg_ntoh32 function, and then reassembling them into the correct host byte order.

The function uses a union to safely access the same memory as both a 64-bit integer and an array of two 32-bit integers. It processes the high-order 32 bits first (from the first array position), shifts them to the upper 32 bits of the result, then adds the converted low-order 32 bits.

This function is essential for receiving 64-bit values (positions, offsets, sizes) from PostgreSQL server responses in large object operations, ensuring proper interpretation regardless of the client platform's byte ordering.

## Parameters / Member Variables
- `net64`: 64-bit integer in network byte order to be converted to host byte order

## Dependencies
- Functions called/Symbols referenced:
  - pg_ntoh32 (called twice for high and low 32-bit halves)
- Called from (representative examples):
  - [lo_lseek64](lo_lseek64.md)
  - [lo_tell64](lo_tell64.md)

## Notes and Other Information
- Returns the 64-bit integer converted to host byte order
- Static function, only accessible within fe-lobj.c
- Uses union type punning to safely access the same data as both 64-bit and 32-bit values
- Processes MSB-first (most significant byte first) network data correctly
- Counterpart to lo_hton64 for bidirectional byte order conversion
- Essential for interpreting 64-bit large object position and size responses from the server
- Handles the conversion by leveraging the existing 32-bit conversion function
- Used specifically in 64-bit variants of large object positioning functions

## Simplified Source

```c
static pg_int64 lo_ntoh64(pg_int64 net64) {
    union {
        pg_int64 i64;
        uint32 i32[2];
    } swap;
    pg_int64 result;

    swap.i64 = net64;

    // Extract and convert high order 32 bits
    result = (uint32) pg_ntoh32(swap.i32[0]);
    result <<= 32;

    // Add converted low order 32 bits
    result |= (uint32) pg_ntoh32(swap.i32[1]);

    return result;
}
```
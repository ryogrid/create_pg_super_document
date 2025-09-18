# lo_hton64

## Location
src/interfaces/libpq/fe-lobj.c: 1023 - 1047

## Overview
Static utility function that converts a 64-bit integer from host byte order to network byte order for large object network protocol communications.

## Definition
```c
static pg_int64 lo_hton64(pg_int64 host64)
```

## Detailed Description
This static function performs byte order conversion for 64-bit integers, converting from host byte order to network byte order (big-endian). It implements the conversion by splitting the 64-bit value into two 32-bit halves and using the existing pg_hton32 function to convert each half separately, then reassembling them in the correct network order.

The function uses a union to safely manipulate the same memory as both a 64-bit integer and an array of two 32-bit integers, ensuring proper byte ordering for network transmission. The high-order 32 bits are placed first (MSB-first), followed by the low-order 32 bits, conforming to network byte order standards.

This function is essential for 64-bit large object operations that need to communicate seek positions, offsets, and sizes across the network in a platform-independent manner.

## Parameters / Member Variables
- `host64`: 64-bit integer in host byte order to be converted to network byte order

## Dependencies
- Functions called/Symbols referenced:
  - pg_hton32 (called twice for high and low 32-bit halves)
- Called from (representative examples):
  - [lo_truncate64](lo_truncate64.md)
  - [lo_lseek64](lo_lseek64.md)

## Notes and Other Information
- Returns the 64-bit integer converted to network byte order
- Static function, only accessible within fe-lobj.c
- Uses union type punning to safely access the same data as both 64-bit and 32-bit values
- Implements MSB-first (most significant byte first) ordering for network compatibility
- Counterpart to lo_ntoh64 for bidirectional byte order conversion
- Essential for 64-bit large object position and size operations across different architectures
- Handles the conversion by leveraging the existing 32-bit conversion function
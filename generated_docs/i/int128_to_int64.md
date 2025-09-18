# int128_to_int64

## Location
[src/include/common/int128.h:269-276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int128.h#L269-L276)

## Overview
Converts a 128-bit signed integer to a 64-bit signed integer by truncating the high-order bits, potentially losing precision.

## Definition
```c
static inline int64 int128_to_int64(INT128 val)
```

## Detailed Description
This is an inline utility function that performs type conversion from a 128-bit signed integer (`INT128`) to a 64-bit signed integer (`int64`). The conversion is a narrowing operation that truncates the high-order 64 bits of the 128-bit value, keeping only the low-order 64 bits. This can result in data loss if the original INT128 value cannot be represented in the smaller int64 range. The function comment notes that this also works fine for casting down to uint64, as the bit pattern is preserved during the cast.

## Parameters / Member Variables
- `val`: The 128-bit signed integer value to be converted to 64-bit integer

## Dependencies
- Functions called/Symbols referenced:
  - INT128 (type)
- Called from (representative examples):
  - [interval_hash](interval_hash.md) (src/backend/utils/adt/timestamp.c:2605)
  - [interval_hash_extended](interval_hash_extended.md) (src/backend/utils/adt/timestamp.c:2618)

## Notes and Other Information
- This is a static inline function defined in src/include/common/int128.h:94-115
- The conversion can result in data loss if the INT128 value exceeds the range of int64
- The function truncates high-order bits and preserves only the low-order 64 bits
- Primarily used in hash function implementations where the full precision of INT128 is not needed
- The comment explicitly mentions this works for casting to uint64 as well, since the bit pattern is preserved
- Callers should be aware that this is a potentially lossy conversion and handle overflow cases appropriately
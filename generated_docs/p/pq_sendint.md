# pq_sendint

## Location
[src/include/libpq/pqformat.h:171-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/pqformat.h#L171-L209)

## Overview
A deprecated inline function that appends a binary integer to a StringInfo buffer with variable byte width support.

## Definition

```c
static inline void
pq_sendint(StringInfo buf, uint32 i, int b)
```
## Detailed Description
The  function is a utility function that appends binary integer data to a StringInfo buffer. It acts as a dispatcher that calls the appropriate type-specific function based on the requested byte width. The function supports 1-byte, 2-byte, and 4-byte integer serialization.

This function is marked as deprecated in the source code comments, with a recommendation to use the more specific functions (, , ) directly instead of this generic wrapper.

The function uses a switch statement to determine which specific serialization function to call based on the byte width parameter. If an unsupported byte width is provided, it raises an ERROR using .

## Parameters / Member Variables
- : StringInfo buffer where the binary integer data will be appended
- : The 32-bit unsigned integer value to be serialized
- : The number of bytes to use for serialization (1, 2, or 4)

## Dependencies
- Functions called/Symbols referenced:
  - [pq_sendint8](pq_sendint8.md)
  - [pq_sendint16](pq_sendint16.md)  
  - [pq_sendint32](pq_sendint32.md)
  - elog (for error handling)
- Called from (representative examples):
  - [logicalrep_write_tuple](../l/logicalrep_write_tuple.md)
  - [string_agg_serialize](../s/string_agg_serialize.md)

## Notes and Other Information
- This function is deprecated and should be avoided in new code
- Prefer using the specific type-safe functions (, , ) directly
- The function will raise an ERROR for unsupported byte widths (anything other than 1, 2, or 4)
- Defined as a static inline function in the header file for performance
- Part of the PostgreSQL libpq message formatting infrastructure used for network protocol communication
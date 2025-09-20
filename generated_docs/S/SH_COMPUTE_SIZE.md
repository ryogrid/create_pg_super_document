# SH_COMPUTE_SIZE

## Location
[src/include/lib/simplehash.h:311-336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/simplehash.h#L311-L336)

## Overview
Computes the optimal hash table size for a PostgreSQL simple hash table, rounding up to the next power of 2 and performing validation checks.

## Definition

```c
static inline uint64
SH_COMPUTE_SIZE(uint64 newsize)
```
## Detailed Description
SH_COMPUTE_SIZE is a macro that generates a function name for computing optimal hash table sizes within PostgreSQL's simple hash table framework. The actual implementation rounds up the requested size to the next power of 2, ensures a minimum size of 2, and validates that the resulting allocation would not exceed platform memory limits. This function is critical for maintaining the hash table's performance characteristics, as power-of-2 sizing enables efficient modulo operations using bitwise AND with a size mask.

## Parameters / Member Variables
- `newsize`: The requested number of elements for the hash table

## Dependencies
- Functions called/Symbols referenced:
  - Max (macro for maximum value)
  - [pg_nextpower2_64](../p/pg_nextpower2_64.md) (rounds up to next power of 2)
  - Assert (assertion macro)
  - SIZE_MAX (maximum size_t value)
  - SH_MAX_SIZE (maximum hash table size constant)
  - SH_ELEMENT_TYPE (hash table element type)
  - sh_error (error reporting function)
  - unlikely (branch prediction hint)
- Called from (representative examples):
  - [SH_UPDATE_PARAMETERS](SH_UPDATE_PARAMETERS.md)
  - SH_CREATE
  - [SH_GROW](SH_GROW.md)

## Notes and Other Information
- Always returns a power of 2 size, which is essential for the hash table's bucketing algorithm
- Includes overflow protection to prevent allocation failures on platforms with limited memory
- Part of PostgreSQL's templated simple hash table implementation where SH_PREFIX defines the specific hash table type
- The minimum size of 2 prevents edge cases with zero-sized hash tables
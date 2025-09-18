# pg_prng_int64_range

## Location
[src/common/pg_prng.c:192-226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_prng.c#L192-L226)

## Overview
Generates a random 64-bit signed integer uniformly distributed within the specified range [rmin, rmax].

## Definition


## Detailed Description
This function selects a random int64 uniformly from the closed interval [rmin, rmax]. If the range is empty (rmax <= rmin), it always returns rmin. The implementation uses pg_prng_uint64_range() internally to ensure uniform distribution, carefully handling the conversion between signed and unsigned 64-bit integers to avoid implementation-defined behavior for large values exceeding PG_INT64_MAX.

The function performs safe type conversions by:
1. Converting the range to unsigned arithmetic to use pg_prng_uint64_range()
2. Adding the random offset to the minimum value
3. Converting back to signed int64 with special handling for values larger than PG_INT64_MAX

## Parameters / Member Variables
- : Pointer to the pseudo-random number generator state structure
- : Minimum value of the range (inclusive)
- : Maximum value of the range (inclusive)

## Dependencies
- Functions called/Symbols referenced:
  - pg_prng_uint64_range
  - likely (optimization hint macro)
  - PG_INT64_MAX
  - PG_INT64_MIN
- Called from (representative examples):
  - [int4random](../i/int4random.md) (in pseudorandomfuncs.c)
  - [int8random](../i/int8random.md) (in pseudorandomfuncs.c)

## Notes and Other Information
- Uses the likely() macro for branch prediction optimization when rmax > rmin
- Handles edge case where range is empty by returning rmin
- Carefully avoids implementation-defined behavior for signed integer overflow
- Modern compilers will optimize the safe conversion logic to a simple assignment when possible
- Part of PostgreSQL's common PRNG interface for consistent random number generation across the system
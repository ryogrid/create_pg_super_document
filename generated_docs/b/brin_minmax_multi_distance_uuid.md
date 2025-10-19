# brin_minmax_multi_distance_uuid

## Location
[src/backend/access/brin/brin_minmax_multi.c:2047-2079](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L2047-L2079)

## Overview
Computes an approximate distance between two UUID values for BRIN minmax multi indexes using a 64-bit float approximation.

## Definition
```c
Datum brin_minmax_multi_distance_uuid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function calculates an approximate distance between two UUID range boundaries in BRIN minmax multi indexes. Since UUIDs are 128-bit values and computing exact deltas would require 128-bit integer arithmetic, the function approximates the distance using 64-bit floating-point arithmetic.

The algorithm iterates through each byte of the UUID from the most significant to least significant byte, computing the difference and scaling it down by dividing by 256 for each byte position. This creates a weighted approximation where more significant bytes contribute more to the final distance value.

The approximation is sufficient for BRIN index purposes, where perfect accuracy is not required and the main goal is to identify reasonably close ranges for merging decisions.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0 (a1): First UUID value (lower bound)
  - Argument 1 (a2): Second UUID value (upper bound)

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetUUIDP](../D/DatumGetUUIDP.md): Extract pg_uuid_t pointer from Datum
  - `DirectFunctionCall2`: PostgreSQL direct function call mechanism
  - [uuid_le](../u/uuid_le.md): UUID less-than-or-equal comparison function
  - [pg_uuid_t](../p/pg_uuid_t.md): PostgreSQL UUID data type structure
  - `UUID_LEN`: Constant defining UUID length in bytes
  - `PG_RETURN_FLOAT8`: PostgreSQL return float8 value macro
- Called from (representative examples):
  - No direct references found (likely referenced through function pointers in BRIN operator classes)

## Notes and Other Information
- Uses approximation to avoid 128-bit integer arithmetic for performance reasons
- The approximation iterates from most significant byte to least significant byte
- Small inaccuracies are acceptable as they only affect range merging decisions in worst case
- Includes assertion checking to validate input ordering and non-negative result
- Returns distance as a float8 value for consistency across BRIN distance functions
- Part of the BRIN minmax multi access method implementation
- Located in src/backend/access/brin/brin_minmax_multi.c:2047-2079

## Simplified Source

```c
Datum brin_minmax_multi_distance_uuid(PG_FUNCTION_ARGS) {
    float8 delta = 0;

    // Extract UUID arguments
    Datum a1 = PG_GETARG_DATUM(0);
    Datum a2 = PG_GETARG_DATUM(1);
    pg_uuid_t *u1 = DatumGetUUIDP(a1);
    pg_uuid_t *u2 = DatumGetUUIDP(a2);

    // Approximate delta using weighted byte differences
    // Process from most significant to least significant byte
    for (int i = UUID_LEN - 1; i >= 0; i--) {
        delta += (int) u2->data[i] - (int) u1->data[i];
        delta /= 256;  // Scale down for next byte position
    }

    PG_RETURN_FLOAT8(delta);
}
```
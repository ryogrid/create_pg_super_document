# brin_minmax_multi_distance_float4

## Location
[src/backend/access/brin/brin_minmax_multi.c:1883-1908](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L1883-L1908)

## Overview
This function computes the distance between two float4 values for use in BRIN minmax-multi index range compaction decisions, with special handling for NaN values.

## Definition

```c
Datum
brin_minmax_multi_distance_float4(PG_FUNCTION_ARGS)
```
## Detailed Description
This is a PostgreSQL function that serves as the distance function for float4 data types in the BRIN minmax-multi operator class. The function is crucial for range compaction operations, as it determines which ranges are closest together and should be merged first during space optimization.

The function handles several special cases for proper float4 distance calculation:

1. **Both values are NaN**: Returns distance 0.0, treating NaN values as equivalent for compaction purposes
2. **One value is NaN**: Returns positive infinity, making NaN ranges least likely to be combined with non-NaN ranges
3. **Normal values**: Performs simple subtraction (a2 - a1) after casting to double precision for accuracy

The function assumes that the input values represent range boundaries where a1 ≤ a2, which is enforced by an assertion. This assumption reflects its usage context where the values come from range min/max boundaries. For collapsed ranges (single points), a1 equals a2, resulting in a distance of 0.0.

The distance calculation is used by the range compaction algorithm to identify adjacent ranges with the smallest gaps, prioritizing their combination to minimize information loss during space optimization.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro which provides access to:
  - Argument 0: First float4 value (a1) - the lower boundary
  - Argument 1: Second float4 value (a2) - the upper boundary

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4 (for extracting function arguments)
  - isnan (for NaN detection)
  - [get_float8_infinity](../g/get_float8_infinity.md) (for infinite distance values)
  - PG_RETURN_FLOAT8 (for returning the result)
- Called from:
  - (No direct references found - likely called through PostgreSQL's operator class system during compaction)

## Notes and Other Information
- Returns a float8 (double precision) value representing the distance between the two input values
- The function follows PostgreSQL's V1 calling convention using PG_FUNCTION_ARGS
- NaN handling ensures that ranges containing NaN values are treated consistently during compaction
- The assertion enforces the expected input ordering (a1 ≤ a2) but this is only active in debug builds
- Critical for the range merging heuristics in BRIN minmax-multi indexes, determining which ranges to combine based on their proximity
- The cast to double precision prevents potential precision loss during subtraction of float4 values
- Infinite distance for NaN cases prevents inappropriate merging of NaN ranges with normal value ranges
- Used specifically by the distance-based compaction algorithms in build_distances and reduce_expanded_ranges functions
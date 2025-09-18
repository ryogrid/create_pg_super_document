# call_subtype_diff

## Location
[src/backend/utils/adt/rangetypes_gist.c:1788-1799](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L1788-L1799)

## Overview
A convenience function that invokes the type-specific subtype_diff function for range types to calculate the difference between two subtype values.

## Definition
```c
static float8 call_subtype_diff(TypeCacheEntry *typcache, Datum val1, Datum val2)
```

## Detailed Description
This function serves as a wrapper to invoke the subtype_diff function associated with a range type. It uses the function information cached in the TypeCacheEntry to call the appropriate subtype difference function with proper collation settings. The function includes error handling to cope with buggy subtype_diff implementations by ensuring the returned value is non-negative, returning 0.0 if the computed difference is negative.

The subtype_diff function is used in GiST index operations to calculate penalties and make splitting decisions based on the "distance" or difference between range boundary values.

## Parameters / Member Variables
- `typcache`: TypeCacheEntry containing cached information about the range type, including the subtype_diff function info and collation
- `val1`: First Datum value for comparison (typically a range boundary)
- `val2`: Second Datum value for comparison (typically a range boundary)

## Dependencies
- Functions called/Symbols referenced:
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [DatumGetFloat8](../D/DatumGetFloat8.md)
- Called from (representative examples):
  - rangeCopy
  - [range_gist_penalty](../r/range_gist_penalty.md)
  - [range_gist_double_sorting_split](../r/range_gist_double_sorting_split.md)
  - [range_gist_consider_split](../r/range_gist_consider_split.md)

## Notes and Other Information
- This is a static function defined in src/backend/utils/adt/rangetypes_gist.c:1788-1799
- The caller must ensure that the range type has a subtype_diff function before calling this function
- Includes defensive programming by checking for non-negative return values and defaulting to 0.0 for invalid results
- Used extensively in GiST index operations for range types to optimize index structure and query performance
- The function leverages PostgreSQL's function call infrastructure with proper collation handling
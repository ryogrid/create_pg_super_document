# range_gist_same

## Location
[src/backend/utils/adt/rangetypes_gist.c:778-820](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L778-L820)

## Overview
The GiST equality comparison method for range types that determines whether two range entries are identical for GiST index operations.

## Definition

```c
Datum
range_gist_same(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the "same" method required by the GiST access method interface for range types. It performs a comprehensive equality check between two range values, ensuring that ranges are considered identical only when they have the same bounds, boundary inclusion flags, and all internal flags.

The function uses a two-stage approach:
1. **Flag comparison**: First compares all flag bits (including RANGE_CONTAIN_EMPTY) to quickly identify unequal ranges
2. **Content comparison**: If flags match, performs detailed equality comparison using the range's element type comparison functions

This is particularly important for GiST index correctness, as it must distinguish between ranges that are semantically equivalent but have different internal representations.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments containing:
  - : First RangeType pointer to compare
  - : Second RangeType pointer to compare  
  - : Pointer to boolean result indicating whether ranges are identical

## Dependencies
- Functions called/Symbols referenced:
  - : Extract range argument from function call
  - : Get flag bits from range
  - : Get type cache for range operations
  - : Get OID of range type
  - : Perform detailed equality comparison
- Called from (representative examples):
  - GiST index operations (indirectly through function pointer in opclass)

## Notes and Other Information
- Located in src/backend/utils/adt/rangetypes_gist.c:778-820
- Essential for GiST index correctness and consistency
- More strict than regular range equality as it checks all flag bits including internal flags
- The RANGE_CONTAIN_EMPTY flag requires special handling as it's ignored by standard range_eq
- Used during GiST index maintenance operations to determine if entries are identical
# range_eq_internal

## Location
src/backend/utils/adt/rangetypes.c: 573 - 604

## Overview
This internal PostgreSQL function performs equality comparison between two range types, implementing the core logic for determining if two ranges are identical.

## Definition


## Detailed Description
The  function is the core implementation for range equality comparison in PostgreSQL. It performs a comprehensive comparison of two range values by first validating that they are of the same range type, then deserializing both ranges to extract their bounds and empty status. The function implements equality semantics where two ranges are equal if: (1) both are empty ranges, or (2) both are non-empty ranges with identical lower and upper bounds (including boundary inclusiveness/exclusiveness). The comparison uses type-specific comparison functions through the type cache system to handle different element types properly.

## Parameters / Member Variables
- : Type cache entry containing comparison functions and metadata for the range's element type
- : First range value to compare (const RangeType *)
- : Second range value to compare (const RangeType *)

## Dependencies
- Functions called/Symbols referenced:
  - RangeTypeGetOid
  - range_deserialize
  - range_cmp_bounds
  - elog (for error reporting)
- Types referenced:
  - RangeBound
  - TypeCacheEntry
  - RangeType
- Called from (representative examples):
  - range_eq
  - range_ne_internal
  - range_gist_same
  - range_gist_consistent_leaf_range
  - spg_range_quad_leaf_consistent

## Notes and Other Information
- This is an internal function used by both public range equality operators and internal PostgreSQL indexing mechanisms
- The function includes a type safety check to prevent comparison of different range types, which should normally be prevented by PostgreSQL's type system
- Empty ranges are considered equal regardless of any bounds they might have stored
- The bounds comparison is delegated to  which handles the complexity of comparing bounds with different inclusiveness flags
- Located in src/backend/utils/adt/rangetypes.c:573-604
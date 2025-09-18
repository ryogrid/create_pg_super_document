# range_super_union

## Location
[src/backend/utils/adt/rangetypes_gist.c:821-887](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L821-L887)

## Overview
A specialized range union function for GiST indexes that computes the smallest range containing two input ranges while tracking empty range information for indexing optimization.

## Definition


## Detailed Description
This static function is a critical component of the GiST range indexing infrastructure. It differs from the regular range_union function in two important ways:

1. **Non-adjacent tolerance**: Unlike regular range union which throws errors for non-adjacent ranges, this function absorbs intervening values into the result range, making it suitable for index node bounding boxes.

2. **Empty range tracking**: It meticulously tracks whether any empty range has been incorporated into the result using the RANGE_CONTAIN_EMPTY flag. This enables efficient indexed searches for contained_by operations.

The function implements an optimized algorithm:
- Handles empty range cases first by preserving or setting the RANGE_CONTAIN_EMPTY flag
- For non-empty ranges, selects the minimum lower bound and maximum upper bound
- Includes optimization to avoid constructing new ranges when one input already represents the result
- Ensures all GiST union operations preserve empty range semantics

## Parameters / Member Variables
- : TypeCacheEntry for the range's element type, providing comparison and type information
- : First RangeType input to union
- : Second RangeType input to union
- Returns: RangeType pointer representing the smallest range containing both inputs

## Dependencies
- Functions called/Symbols referenced:
  - : Extract bounds and empty status from range
  - : Get flag bits from range
  - : Create a copy of a range
  - : Set the RANGE_CONTAIN_EMPTY flag
  - : Compare range bounds
  - : Construct new range from bounds
- Called from (representative examples):
  - : GiST union method for range types
  - Other GiST internal operations

## Notes and Other Information
- Located in src/backend/utils/adt/rangetypes_gist.c:821-887
- Static function used exclusively within the GiST range implementation
- Essential for maintaining correct bounding box semantics in GiST range indexes
- The empty range tracking is crucial for supporting contained_by (@>) operator indexing
- Implements important optimizations to avoid unnecessary range construction
- All GiST union operations for ranges must go through this function to maintain consistency
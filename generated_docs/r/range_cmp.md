# range_cmp

## Location
[src/backend/utils/adt/rangetypes.c:1249-1294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L1249-L1294)

## Overview
B-tree comparator function that provides total ordering for range types, enabling range values to be sorted, indexed, and used in ordered operations.

## Definition

```c
Datum
range_cmp(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the primary comparison logic for PostgreSQL range types, providing the foundation for B-tree indexing and ordering operations. It establishes a total ordering among range values by comparing them lexicographically: first by lower bounds, then by upper bounds if the lower bounds are equal.

The function handles several special cases in its comparison logic: empty ranges are considered to sort before all non-empty ranges, and when comparing two empty ranges, they are considered equal. For non-empty ranges, the comparison is performed using type-specific comparison functions for the range's element type.

The function includes a stack depth check to prevent stack overflow when dealing with nested range types (ranges whose element type is itself a range type). It also includes proper memory management by freeing copied range values when they are no longer needed.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument (index 0): First range value for comparison ()
  - Second argument (index 1): Second range value for comparison ()

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts range arguments from function call
  - : Prevents stack overflow in recursive comparisons
  - : Gets the OID of a range type for type validation
  - : Retrieves type cache information for element type operations
  - : Extracts boundary and empty flag information from ranges
  - : Compares individual range boundaries using element type comparison
  - : Releases memory for copied range arguments
  - : Returns the comparison result as a 32-bit integer
  - : Structure representing range boundary information
- Called from:
  - : Less-than comparison function
  - : Less-than-or-equal comparison function  
  - : Greater-than-or-equal comparison function
  - : Greater-than comparison function
  - B-tree indexing operations on range columns
  - ORDER BY clauses involving range values

## Notes and Other Information
- Returns negative, zero, or positive integer indicating r1 < r2, r1 = r2, or r1 > r2 respectively
- Empty ranges sort before all non-empty ranges in the ordering
- Comparison is lexicographic: lower bounds compared first, then upper bounds if lower bounds are equal
- Includes type checking to ensure both ranges are of the same range type
- Handles recursive range types safely with stack depth checking
- Essential for B-tree indexing support and range ordering operations
- Located in
- Forms the basis for all range comparison operators and sorting functionality

## Simplified Source

```c
Datum range_cmp(PG_FUNCTION_ARGS) {
    RangeType *r1 = PG_GETARG_RANGE_P(0);
    RangeType *r2 = PG_GETARG_RANGE_P(1);
    RangeBound lower1, lower2, upper1, upper2;
    bool empty1, empty2;
    int cmp;

    // Prevent stack overflow for recursive range types
    check_stack_depth();

    // Validate that both ranges are of the same type
    if (RangeTypeGetOid(r1) != RangeTypeGetOid(r2))
        elog(ERROR, "range types do not match");

    TypeCacheEntry *typcache = range_get_typcache(fcinfo, RangeTypeGetOid(r1));

    // Extract boundaries from both ranges
    range_deserialize(typcache, r1, &lower1, &upper1, &empty1);
    range_deserialize(typcache, r2, &lower2, &upper2, &empty2);

    // Handle empty range comparison (empty ranges sort first)
    if (empty1 && empty2)
        cmp = 0;
    else if (empty1)
        cmp = -1;
    else if (empty2)
        cmp = 1;
    else {
        // Lexicographic comparison: lower bounds first, then upper bounds
        cmp = range_cmp_bounds(typcache, &lower1, &lower2);
        if (cmp == 0)
            cmp = range_cmp_bounds(typcache, &upper1, &upper2);
    }

    // Clean up memory and return comparison result
    PG_FREE_IF_COPY(r1, 0);
    PG_FREE_IF_COPY(r2, 1);
    PG_RETURN_INT32(cmp);
}
```
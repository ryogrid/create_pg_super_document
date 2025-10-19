# btbpchar_pattern_cmp

## Location
[src/backend/utils/adt/varchar.c:1205-1220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L1205-L1220)

## Overview
Implements the comparison support function for B-tree indexes on BPCHAR (blank-padded character) values using pattern-based comparison, essential for creating indexes that support LIKE clause operations.

## Definition
```c
Datum btbpchar_pattern_cmp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the comparison support function for B-tree indexes on BPCHAR data types when pattern-based ordering is required. Unlike the boolean comparison operators (lt, le, ge, gt), this function returns the actual comparison result as an integer, which is essential for B-tree index construction and maintenance.

The function performs character-by-character comparison of two BPCHAR values and returns a negative value if the first argument is less than the second, zero if they are equal, and a positive value if the first argument is greater than the second. This three-way comparison result enables efficient B-tree operations for pattern matching queries.

## Parameters / Member Variables
- `arg1`: First BPCHAR value to compare (extracted from PG_FUNCTION_ARGS at position 0)
- `arg2`: Second BPCHAR value to compare (extracted from PG_FUNCTION_ARGS at position 1)
- `result`: Integer comparison result from the internal comparison function

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BPCHAR_PP (macro to extract BPCHAR arguments)
  - [internal_bpchar_pattern_compare](../i/internal_bpchar_pattern_compare.md) (performs the actual comparison)
  - PG_FREE_IF_COPY (macro to free memory if needed)
  - PG_RETURN_INT32 (macro to return 32-bit integer result)
- Called from (representative examples):
  - B-tree index creation and maintenance operations
  - PostgreSQL index access methods for pattern-based queries
  - [Query](../Q/Query.md) planner for index selection with LIKE clauses

## Notes and Other Information
- This function is the core comparison function used by B-tree indexes for pattern-based BPCHAR ordering
- Returns the raw comparison result (-1, 0, or 1) rather than a boolean value like the other pattern comparison functions
- Essential for building indexes that can efficiently support LIKE pattern matching queries
- The comparison is compatible with "C" collation and character-by-character comparison semantics
- Memory management follows PostgreSQL conventions with PG_FREE_IF_COPY for proper cleanup

## Simplified Source

```c
Datum btbpchar_pattern_cmp(PG_FUNCTION_ARGS) {
    // Extract the two BPCHAR arguments
    BpChar *arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar *arg2 = PG_GETARG_BPCHAR_PP(1);

    // Perform pattern-based comparison
    int result = internal_bpchar_pattern_compare(arg1, arg2);

    // Clean up memory if needed
    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    // Return the raw comparison result (-1, 0, or 1)
    PG_RETURN_INT32(result);
}
```

**Key Points:**
- B-tree comparison support function for pattern-based BPCHAR indexing
- Returns integer result (-1, 0, 1) rather than boolean like other operators
- Essential for building B-tree indexes that support LIKE clause operations
- Uses binary comparison (not locale-aware) compatible with "C" collation
- Enables efficient index operations for pattern matching queries
# bpchar_pattern_le

## Location
[src/backend/utils/adt/varchar.c:1157-1172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L1157-L1172)

## Overview
Implements the "less than or equal to" operator for pattern-based comparison of BPCHAR (blank-padded character) values, supporting character-by-character comparison suitable for LIKE clause indexing.

## Definition
```c
Datum bpchar_pattern_le(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs a pattern-based "less than or equal to" comparison between two BPCHAR values. It follows the same character-by-character comparison approach as other pattern comparison functions, making it suitable for building indexes to support LIKE clauses and pattern matching operations.

The function extracts two BPCHAR arguments from the PostgreSQL function call interface, delegates the actual comparison logic to `internal_bpchar_pattern_compare`, and returns true if the first argument is lexicographically less than or equal to the second argument.

## Parameters / Member Variables
- `arg1`: First BPCHAR value to compare (extracted from PG_FUNCTION_ARGS at position 0)
- `arg2`: Second BPCHAR value to compare (extracted from PG_FUNCTION_ARGS at position 1)  
- `result`: Integer result from the internal comparison function

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BPCHAR_PP (macro to extract BPCHAR arguments)
  - [internal_bpchar_pattern_compare](../i/internal_bpchar_pattern_compare.md) (performs the actual comparison)
  - PG_FREE_IF_COPY (macro to free memory if needed)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - PostgreSQL query planner and executor for pattern-based comparisons
  - B-tree index operations for LIKE clause support

## Notes and Other Information
- This function is part of a family of pattern comparison operators that support character-by-character comparison
- The comparison is compatible with regular bpchareq/bpcharne operators and support functions when using "C" collation
- Memory management is handled through PG_FREE_IF_COPY macros to prevent memory leaks
- Returns true when the comparison result is <= 0, covering both less than and equal cases

## Simplified Source

```c
Datum bpchar_pattern_le(PG_FUNCTION_ARGS) {
    // Extract the two BPCHAR arguments
    BpChar *arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar *arg2 = PG_GETARG_BPCHAR_PP(1);

    // Perform pattern-based comparison
    int result = internal_bpchar_pattern_compare(arg1, arg2);

    // Clean up memory if needed
    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    // Return true if arg1 <= arg2
    PG_RETURN_BOOL(result <= 0);
}
```

**Key Points:**
- Implements "less than or equal to" operator for pattern-based BPCHAR comparison
- Uses binary comparison (not locale-aware) suitable for LIKE indexes
- Delegates actual comparison to `internal_bpchar_pattern_compare()`
- Handles memory management with `PG_FREE_IF_COPY` macros
- Returns true if first argument is lexicographically smaller or equal
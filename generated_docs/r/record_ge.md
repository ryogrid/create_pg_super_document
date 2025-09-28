# record_ge

## Location
[src/backend/utils/adt/rowtypes.c:1307-1312](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L1307-L1312)

## Overview
The `record_ge` function implements the "greater than or equal to" comparison operator for PostgreSQL record (composite) types.

## Definition
```c
Datum record_ge(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the PostgreSQL SQL function implementation for the >= operator when comparing record/composite types. It delegates the actual comparison logic to the `record_cmp` function and returns true if the first record is greater than or equal to the second record according to PostgreSQL's record comparison semantics.

The function follows PostgreSQL's standard function calling convention using `PG_FUNCTION_ARGS` and returns a boolean result via `PG_RETURN_BOOL`. The comparison result is determined by checking if `record_cmp` returns a value >= 0.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing the two records to be compared

## Dependencies
- Functions called/Symbols referenced:
  - [record_cmp](record_cmp.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's type system for composite/record types
- The actual comparison logic is implemented in `record_cmp`, which performs field-by-field comparison
- Returns true when the first record is greater than or equal to the second record
- Located in src/backend/utils/adt/rowtypes.c at lines 1307-1312

## Simplified Source

```c
// Simplified version of record_ge
Datum record_ge(PG_FUNCTION_ARGS) {
    // Use record_cmp and check if first record is greater than or equal to second
    PG_RETURN_BOOL(record_cmp(fcinfo) >= 0);
}
```

Key simplifications made:
- The function is already extremely simple - just checks if record_cmp returns >= 0
- No additional simplification needed as it's a one-line wrapper
- Delegates all comparison logic to record_cmp for consistency and maintainability
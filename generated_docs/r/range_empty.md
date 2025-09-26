# range_empty

## Location
[src/backend/utils/adt/rangetypes.c:491-500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L491-L500)

## Overview
Determines whether a range type is empty, returning a boolean value indicating if the range contains no elements.

## Definition
Datum range_empty(PG_FUNCTION_ARGS)

## Detailed Description
The `range_empty` function is a range predicate function that checks whether a PostgreSQL range type is empty. It works by extracting the flags from the range header and testing for the `RANGE_EMPTY` flag bit. This is an efficient operation as it only needs to examine the range header without deserializing the entire range structure. An empty range represents a set with no elements and is distinct from a range with infinite bounds.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `r1` (RangeType *): Input range to test for emptiness

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_RANGE_P` - Extracts range argument from function parameters
  - [range_get_flags](range_get_flags.md) - Retrieves the flags byte from the range header
  - `RANGE_EMPTY` - Flag constant indicating an empty range
- Called from (representative examples):
  - SQL queries using the `isempty()` function on range types
  - [Range](../R/Range.md) validation and conditional operations

## Notes and Other Information
- This is a very efficient operation that only examines the range header flags
- Empty ranges are created when the lower bound is greater than or equal to the upper bound (depending on inclusiveness)
- Returns true for ranges that contain no elements, false otherwise
- Part of the standard range predicate function family
- Commonly used in conditional logic and range validation operations
# cideq

## Location
[src/backend/utils/adt/xid.c:370-376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid.c#L370-L376)

## Overview
A PostgreSQL internal function that performs equality comparison between two CommandId (cid) values.

## Definition
```c
Datum cideq(PG_FUNCTION_ARGS)
```

## Detailed Description
The `cideq` function is part of PostgreSQL's operator infrastructure for the CommandId type. It provides the equality comparison operation for CommandId values, which is fundamental for various database operations including indexing, sorting, and query processing. The function takes two CommandId arguments and returns a boolean value indicating whether they are equal. This simple but essential operation enables CommandId values to be used in WHERE clauses, JOIN conditions, and other SQL constructs that require equality testing.

## Parameters / Member Variables
- Input: Two CommandId values retrieved via `PG_GETARG_COMMANDID(0)` and `PG_GETARG_COMMANDID(1)`
- Output: Returns a boolean result via `PG_RETURN_BOOL` indicating equality

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_COMMANDID (macro for extracting CommandId from function args)
  - CommandId (PostgreSQL internal type)
  - PG_RETURN_BOOL (macro for returning boolean result)

- Called from (representative examples):
  - No direct references found in the codebase (likely called through PostgreSQL's operator system)

## Notes and Other Information
- Located in src/backend/utils/adt/xid.c:370-376
- Part of the CommandId type's operator function suite
- Implements the = operator for CommandId type
- Uses simple integer comparison since CommandId is essentially an integer type
- Essential for CommandId values to participate in SQL equality operations
- Follows PostgreSQL's standard pattern for comparison functions using the PG_FUNCTION_ARGS interface

## Simplified Source

```c
Datum cideq(PG_FUNCTION_ARGS) {
    // Get the two CommandIds to compare
    CommandId arg1 = PG_GETARG_COMMANDID(0);
    CommandId arg2 = PG_GETARG_COMMANDID(1);

    // Return true if equal, false otherwise
    PG_RETURN_BOOL(arg1 == arg2);
}
```
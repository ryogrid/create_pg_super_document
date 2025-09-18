# contain_volatile_functions_not_nextval_checker

## Location
[src/backend/optimizer/util/clauses.c:679-685](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L679-L685)

## Overview
A static helper function that checks whether a given function is volatile while specifically excluding nextval() from being considered volatile.

## Definition
```c
static bool contain_volatile_functions_not_nextval_checker(Oid func_id, void *context)
```

## Detailed Description
This function serves as a specialized checker function used by the tree walker infrastructure to determine volatility of functions with special handling for `nextval()`. It implements the core logic for the COPY-specific volatility checking by:

1. Explicitly excluding `F_NEXTVAL` (the nextval function) from being considered volatile
2. For all other functions, checking if they have `PROVOLATILE_VOLATILE` volatility level using `func_volatile()`

The function returns true only if the function is volatile AND is not nextval(), enabling COPY operations to handle sequence operations differently from other volatile functions.

## Parameters / Member Variables
- `func_id`: The OID of the function to check for volatility
- `context`: Context parameter (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [func_volatile](../f/func_volatile.md): Retrieves the volatility level of the specified function
  - `PROVOLATILE_VOLATILE`: Constant representing volatile function level
- Called from (representative examples):
  - [contain_volatile_functions_not_nextval_walker](contain_volatile_functions_not_nextval_walker.md) (at clauses.c:692)

## Notes and Other Information
- This is a static function, only visible within the clauses.c compilation unit
- Designed to work with PostgreSQL's tree walker infrastructure
- The special treatment of F_NEXTVAL reflects COPY operation requirements
- Returns true only for volatile functions that are not nextval()
- Part of the specialized volatility checking system for bulk operations
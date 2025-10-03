# contain_mutable_functions_checker

## Location
[src/backend/optimizer/util/clauses.c:376-381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L376-L381)

## Overview
A helper function that checks whether a specific function is mutable by examining its volatility classification.

## Definition

```c
static bool
contain_mutable_functions_checker(Oid func_id, void *context)
```
## Detailed Description
This static helper function serves as a callback used by  to determine if a specific function is mutable (non-immutable). It works by querying the function's volatility classification from PostgreSQL's system catalogs.

The function considers a function to be mutable if its volatility level is anything other than . This means functions classified as either  (results can change within a transaction) or  (results can change between any two calls) are considered mutable.

PostgreSQL's function volatility classifications:
- **IMMUTABLE**: Function always returns the same result given the same arguments (e.g., mathematical functions)
- **STABLE**: Function results can change within a transaction but are stable within a statement (e.g., )
- **VOLATILE**: Function results can change between any two calls (e.g., , )

This function is designed to be used as a callback with PostgreSQL's function checking infrastructure.

## Parameters / Member Variables
- `func_id`: The OID of the function to check for mutability
- `*context`: Context parameter (currently unused)
## Dependencies
- Functions called/Symbols referenced:
  - [func_volatile](../f/func_volatile.md)
  - PROVOLATILE_IMMUTABLE (constant)
- Called from (representative examples):
  - [contain_mutable_functions_walker](contain_mutable_functions_walker.md)

## Notes and Other Information
- This is a static function, only accessible within the same source file
- Used as a callback function in the function checking infrastructure
- Returns true for both STABLE and VOLATILE functions (any non-IMMUTABLE function)
- Part of the expression analysis system that prevents incorrect constant folding
- The context parameter is unused but maintained for callback compatibility

## Simplified Source

```c
static bool
contain_mutable_functions_checker(Oid func_id, void *context)
{
    // Return true if function is not immutable (i.e., is mutable)
    return (func_volatile(func_id) != PROVOLATILE_IMMUTABLE);
}
```
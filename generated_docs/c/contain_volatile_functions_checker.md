# contain_volatile_functions_checker

## Location
[src/backend/optimizer/util/clauses.c:544-549](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L544-L549)

## Overview
A simple checker function that determines if a specific function is volatile by examining its volatility category in the PostgreSQL system catalogs.

## Definition
```c
static bool contain_volatile_functions_checker(Oid func_id, void *context)
```

## Detailed Description
This function serves as a callback predicate for the function checking infrastructure in PostgreSQL's optimizer. It provides a precise test for volatility by checking if a function's volatility category is exactly PROVOLATILE_VOLATILE.

The function uses `func_volatile()` to query the system catalog for the function's declared volatility level and compares it against the PROVOLATILE_VOLATILE constant. This is more restrictive than the mutable function checker, which considers both STABLE and VOLATILE functions as mutable, whereas this function only identifies truly volatile functions.

PostgreSQL's volatility categories:
- **IMMUTABLE**: Function cannot modify database and always returns same result for same inputs
- **STABLE**: Function cannot modify database but may return different results for same inputs within a single statement
- **VOLATILE**: Function may modify database and/or return different results for same inputs

## Parameters / Member Variables
- `func_id`: The OID of the function to check for volatility
- `context`: Unused context pointer (required for callback interface compatibility)

## Dependencies
- Functions called/Symbols referenced:
  - [func_volatile](../f/func_volatile.md)
  - PROVOLATILE_VOLATILE
- Called from (representative examples):
  - [contain_volatile_functions_walker](contain_volatile_functions_walker.md)

## Notes and Other Information
- Returns `true` only for functions marked as PROVOLATILE_VOLATILE, not for STABLE functions
- Part of the callback-based function checking infrastructure used by `check_functions_in_node`
- More specific than `contain_mutable_functions_checker` which treats both STABLE and VOLATILE as mutable
- Static function, indicating it's an internal implementation detail of the volatility checking system
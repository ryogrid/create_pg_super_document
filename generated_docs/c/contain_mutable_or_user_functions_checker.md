# contain_mutable_or_user_functions_checker

## Location
[src/backend/commands/publicationcmds.c:438-482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L438-L482)

## Overview
A callback function that checks whether a given function is mutable or user-defined, used to validate expressions in publication WHERE clauses.

## Definition
```c
static bool contain_mutable_or_user_functions_checker(Oid func_id, void *context)
```

## Detailed Description
This function serves as a callback for the check_functions_in_node infrastructure to determine if a function should be rejected in publication WHERE clauses. It returns true if the function is either:
1. Not immutable (i.e., volatile or stable)
2. A user-defined function (has an OID >= FirstNormalObjectId)

The function is designed to enforce restrictions on publication WHERE clauses by ensuring only immutable system functions are allowed, which is necessary for consistent replication behavior.

## Parameters / Member Variables
- `func_id`: The OID of the function to check
- `context`: Unused context parameter (required by callback interface)

## Dependencies
- Functions called/Symbols referenced:
  - [func_volatile](../f/func_volatile.md)
  - PROVOLATILE_IMMUTABLE
  - FirstNormalObjectId
- Called from:
  - [check_simple_rowfilter_expr_walker](check_simple_rowfilter_expr_walker.md)

## Notes and Other Information
- This is a static function used internally within publicationcmds.c
- The function helps maintain data consistency in logical replication by preventing non-deterministic functions in WHERE clauses
- User-defined functions are rejected regardless of their volatility to ensure security and predictability

## Simplified Source

```c
static bool
contain_mutable_or_user_functions_checker(Oid func_id, void *context)
{
    // Return true if function should be rejected:
    // 1. Not immutable (volatile or stable)
    // 2. User-defined function (OID >= FirstNormalObjectId)
    return (func_volatile(func_id) != PROVOLATILE_IMMUTABLE ||
            func_id >= FirstNormalObjectId);
}
```
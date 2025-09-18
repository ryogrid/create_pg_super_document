# getSubscriptingRoutines

## Location
[src/backend/utils/cache/lsyscache.c:3130-3157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L3130-L3157)

## Overview
Retrieves the complete subscripting methods structure for a given PostgreSQL data type, providing access to all the function pointers needed to implement subscripting operations.

## Definition
```c
const struct SubscriptRoutines *getSubscriptingRoutines(Oid typid, Oid *typelemp)
```

## Detailed Description
This function builds upon `get_typsubscript()` by not only finding the subscripting handler function OID but also calling that function to retrieve the complete `SubscriptRoutines` structure. The `SubscriptRoutines` structure contains function pointers for various subscripting operations like fetch, assign, and other related operations. The function calls the handler function with no arguments using `OidFunctionCall0()` and returns the resulting structure pointer. If the type has no subscripting support, it returns NULL.

## Parameters / Member Variables
- `typid`: The OID of the PostgreSQL data type to look up
- `typelemp`: Optional output parameter; if not NULL, receives the type's element type OID (`typelem` field)

## Dependencies
- Functions called/Symbols referenced:
  - [get_typsubscript](get_typsubscript.md)
  - OidIsValid
  - OidFunctionCall0
  - [DatumGetPointer](../D/DatumGetPointer.md)
- Called from (representative examples):
  - [ExecInitSubscriptingRef](../E/ExecInitSubscriptingRef.md)
  - [contain_nonstrict_functions_walker](../c/contain_nonstrict_functions_walker.md)
  - [contain_leaked_vars_walker](../c/contain_leaked_vars_walker.md)
  - [transformContainerSubscripts](../t/transformContainerSubscripts.md)

## Notes and Other Information
- Returns NULL if the type is not subscriptable (no valid subscripting handler)
- The returned structure contains function pointers for implementing subscripting semantics
- Used primarily by the executor and parser when processing subscripting expressions
- Enables PostgreSQL's extensible subscripting system for custom types
- The subscripting handler function is expected to return a pointer to a static `SubscriptRoutines` structure
- Critical for array access operations and custom container types
- Located in `src/backend/utils/cache/lsyscache.c:3130-3157`
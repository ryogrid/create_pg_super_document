# pg_stop_making_pinned_objects

## Location
[src/backend/catalog/catalog.c:695-710](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/catalog.c#L695-L710)

## Overview
A SQL-callable function that stops the generation of pinned object IDs during database initialization, transitioning to unpinned object creation.

## Definition

```c
Datum
pg_stop_making_pinned_objects(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a SQL interface wrapper for the  function, designed exclusively for use by initdb during PostgreSQL database initialization. The function transitions the object ID generation mechanism from creating "pinned" objects (which have special system significance and low OIDs) to creating "unpinned" objects (regular user objects with higher OIDs).

The function includes security validation to ensure only superusers can execute it, and it's intentionally not documented in user-facing documentation since it should only be called during database initialization processes.

When called, it forces the global OID counter to advance to , ensuring that all subsequently created objects will have unpinned status. This allows initdb to create essential system objects as pinned during early initialization, then switch to unpinned mode for the remaining initialization objects.

## Parameters / Member Variables
This function follows the PostgreSQL function calling convention:
- Uses  macro for parameter handling
- Takes no actual parameters (void function semantically)
- Returns  type with  

## Dependencies
- Functions called/Symbols referenced:
  -  - Checks if current user has superuser privileges
  -  - Core function that advances OID counter
  -  - PostgreSQL macro for returning void from SQL functions
  -  - PostgreSQL error reporting mechanism
- Called from (representative examples):
  - No direct callers found (intended for initdb SQL execution only)

## Notes and Other Information
- **Restricted Usage**: This function is designed exclusively for initdb and should never be called in normal database operations
- **Security**: Requires superuser privileges with explicit privilege checking
- **Initialization Context**: Part of the database bootstrap and initialization process
- **Object ID Management**: Critical for proper separation between system (pinned) and user (unpinned) objects
- **Error Handling**: Includes comprehensive error reporting for insufficient privileges
- **Documentation Status**: Intentionally undocumented in user manuals due to its specialized internal use case

## Simplified Source

```c
Datum pg_stop_making_pinned_objects(PG_FUNCTION_ARGS) {
    // Security check: only superusers can call this function
    if (!superuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                       errmsg("must be superuser to call %s()",
                              "pg_stop_making_pinned_objects")));
    }

    // Stop generating pinned object IDs and switch to unpinned mode
    StopGeneratingPinnedObjectIds();

    PG_RETURN_VOID();
}
```
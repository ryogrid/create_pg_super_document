# inv_create

## Location
[src/backend/storage/large_object/inv_api.c:211-252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/large_object/inv_api.c#L211-L252)

## Overview
Creates a new large object in PostgreSQL with optional OID specification, establishing ownership dependencies and triggering post-creation hooks.

## Definition

```c
Oid
inv_create(Oid lobjId)
```
## Detailed Description
This function creates a new large object in the PostgreSQL system with initially empty data pages. It can either accept a specific OID to use for the new object or automatically generate one if InvalidOid is provided. The function handles the complete lifecycle of large object creation including: calling the low-level LargeObjectCreate function, establishing ownership dependencies using the current user ID, invoking post-creation hooks for extensibility, and advancing the command counter to ensure the new object is visible to subsequent operations within the same transaction. The dependency tracking uses LargeObjectRelationId for backward compatibility with existing tools like pg_dump.

## Parameters / Member Variables
- : OID to use for the new large object, or InvalidOid to have the system automatically assign an OID

## Dependencies
- Functions called/Symbols referenced:
  - [LargeObjectCreate](../L/LargeObjectCreate.md) (creates the actual large object metadata)
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md) (establishes ownership dependency)
  - [GetUserId](../G/GetUserId.md) (retrieves current user ID for ownership)
  - InvokeObjectPostCreateHook (triggers post-creation hooks)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md) (makes new object visible)
- Called from (representative examples):
  - [be_lo_creat](../b/be_lo_creat.md)
  - [be_lo_create](../b/be_lo_create.md)
  - [lo_import_internal](../l/lo_import_internal.md)
  - [be_lo_from_bytea](../b/be_lo_from_bytea.md)

## Notes and Other Information
- Returns the OID of the newly created large object
- Raises an error if a specific lobjId is requested but already exists
- Uses LargeObjectRelationId for dependency tracking (not LargeObjectMetadataRelationId) for backward compatibility
- Post-creation hooks allow extensions to perform additional processing
- [Command](../C/Command.md) counter increment ensures immediate visibility within the transaction
- Part of the public large object API (not static)
- Integrates with PostgreSQL's dependency tracking system for proper cleanup
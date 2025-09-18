# inv_create

## Location
src/backend/storage/large_object/inv_api.c: 211 - 252

## Overview
Creates a new large object in PostgreSQL with optional OID specification, establishing ownership dependencies and triggering post-creation hooks.

## Definition


## Detailed Description
This function creates a new large object in the PostgreSQL system with initially empty data pages. It can either accept a specific OID to use for the new object or automatically generate one if InvalidOid is provided. The function handles the complete lifecycle of large object creation including: calling the low-level LargeObjectCreate function, establishing ownership dependencies using the current user ID, invoking post-creation hooks for extensibility, and advancing the command counter to ensure the new object is visible to subsequent operations within the same transaction. The dependency tracking uses LargeObjectRelationId for backward compatibility with existing tools like pg_dump.

## Parameters / Member Variables
- : OID to use for the new large object, or InvalidOid to have the system automatically assign an OID

## Dependencies
- Functions called/Symbols referenced:
  - LargeObjectCreate (creates the actual large object metadata)
  - recordDependencyOnOwner (establishes ownership dependency)
  - GetUserId (retrieves current user ID for ownership)
  - InvokeObjectPostCreateHook (triggers post-creation hooks)
  - CommandCounterIncrement (makes new object visible)
- Called from (representative examples):
  - be_lo_creat
  - be_lo_create
  - lo_import_internal
  - be_lo_from_bytea

## Notes and Other Information
- Returns the OID of the newly created large object
- Raises an error if a specific lobjId is requested but already exists
- Uses LargeObjectRelationId for dependency tracking (not LargeObjectMetadataRelationId) for backward compatibility
- Post-creation hooks allow extensions to perform additional processing
- Command counter increment ensures immediate visibility within the transaction
- Part of the public large object API (not static)
- Integrates with PostgreSQL's dependency tracking system for proper cleanup
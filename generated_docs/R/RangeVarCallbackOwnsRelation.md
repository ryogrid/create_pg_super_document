# RangeVarCallbackOwnsRelation

## Location
[src/backend/commands/tablecmds.c:17815-17846](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L17815-L17846)

## Overview
A callback function for RangeVarGetRelidExtended() that verifies the current user is the owner of the relation or is a superuser, ensuring proper authorization before operations on relations.

## Definition


## Detailed Description
This function serves as a security callback that enforces ownership requirements for database relations. It is designed to be used with RangeVarGetRelidExtended() to perform authorization checks during relation lookups. The function performs comprehensive ownership verification by checking both user privileges and system catalog restrictions.

The function first validates that a relation ID was found, then retrieves the relation's metadata from the system catalog. It performs two critical security checks: verifying the user owns the relation (or is superuser) and ensuring system catalogs cannot be modified when system table modifications are disabled.

## Parameters / Member Variables
- : Pointer to RangeVar structure containing the relation name and schema information
- : Object identifier of the relation being accessed  
- : Previous relation ID (used for concurrent operations, ignored in this callback)
- : Generic argument pointer (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [object_ownercheck](../o/object_ownercheck.md) - Verifies user ownership of database objects
  - [aclcheck_error](../a/aclcheck_error.md) - Reports access control violations
  - [get_relkind_objtype](../g/get_relkind_objtype.md) - Determines object type from relation kind
  - [get_rel_relkind](../g/get_rel_relkind.md) - Retrieves relation kind from system catalog
  - [IsSystemClass](../I/IsSystemClass.md) - Checks if relation is a system catalog
- Called from (representative examples):
  - [AlterSequence](../A/AlterSequence.md) (src/backend/commands/sequence.c:457)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1478)

## Notes and Other Information
- The function silently returns if no valid relation ID is provided, allowing higher-level code to handle missing relations appropriately
- System catalog protection is enforced only when allowSystemTableMods is false, providing flexibility for administrative operations
- Uses PostgreSQL's standard error reporting mechanisms (elog, ereport) for consistent error handling
- Part of the table command infrastructure in src/backend/commands/tablecmds.c (lines 17815-17846)
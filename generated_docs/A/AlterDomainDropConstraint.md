# AlterDomainDropConstraint

## Location
[src/backend/commands/typecmds.c:2791-2896](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L2791-L2896)

## Overview
Implements the ALTER DOMAIN DROP CONSTRAINT statement, removing a named constraint from a domain type and updating the domain's metadata accordingly.

## Definition


## Detailed Description
This function removes a named constraint from a domain type by scanning the pg_constraint catalog for the target constraint. It handles special processing for NOT NULL constraints by updating the typnotnull field in pg_type. The function uses a systematic scan of constraints associated with the domain and performs deletion using the specified drop behavior. It also handles cache invalidation to ensure dependent plans are rebuilt since the domain's pg_type row doesn't change automatically.

## Parameters / Member Variables
- : List of qualified names identifying the domain
- : Name of the constraint to drop
- : Drop behavior (CASCADE or RESTRICT) controlling how dependent objects are handled
- : If true, don't error when the constraint doesn't exist, just issue a notice

## Dependencies
- Functions called/Symbols referenced:
  - makeTypeNameFromNameList
  - [typenameTypeId](../t/typenameTypeId.md)
  - SearchSysCacheCopy1
  - [checkDomainOwner](../c/checkDomainOwner.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [performDeletion](../p/performDeletion.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [CacheInvalidateHeapTuple](../C/CacheInvalidateHeapTuple.md)
  - [TypeNameToString](../T/TypeNameToString.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Uses a three-key scan on pg_constraint to find the target constraint efficiently
- Special handling for NOT NULL constraints updates the domain's typnotnull field
- Manually invalidates cache since pg_type row doesn't change for most constraint types
- Supports IF EXISTS semantics through the missing_ok parameter
- Ensures proper locking on both type and constraint relations for consistency
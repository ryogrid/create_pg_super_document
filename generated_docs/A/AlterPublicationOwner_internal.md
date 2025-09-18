# AlterPublicationOwner_internal

## Location
[src/backend/commands/publicationcmds.c:1888-1945](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L1888-L1945)

## Overview
Internal function that performs the core logic for changing a PostgreSQL publication's owner, including permission checks and catalog updates.

## Definition
```c
static void AlterPublicationOwner_internal(Relation rel, HeapTuple tup, Oid newOwnerId)
```

## Detailed Description
This function serves as the internal workhorse for changing publication ownership. It performs comprehensive permission validation to ensure that ownership changes comply with PostgreSQL's security model, particularly for special publication types (FOR ALL TABLES and FOR TABLES IN SCHEMA) that require superuser privileges. The function validates that the current user has appropriate permissions, the new owner can accept ownership, and the new owner has necessary database privileges. Upon successful validation, it updates the publication tuple in the system catalog and maintains dependency relationships.

The function enforces strict security policies: FOR ALL TABLES and FOR TABLES IN SCHEMA publications can only be owned by superusers due to their broad access implications. It also ensures proper dependency tracking by updating ownership dependencies in the system catalogs.

## Parameters / Member Variables
- `rel`: Open relation handle to the pg_publication system catalog
- `tup`: HeapTuple representing the publication record to be modified
- `newOwnerId`: OID of the user who will become the new owner of the publication

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_publication (struct type)
  - superuser
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - check_can_set_role
  - [object_aclcheck](../o/object_aclcheck.md)
  - [get_database_name](../g/get_database_name.md)
  - superuser_arg
  - [is_schema_publication](../i/is_schema_publication.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [changeDependencyOnOwner](../c/changeDependencyOnOwner.md)
  - InvokeObjectPostAlterHook
- Called from (representative examples):
  - [AlterPublicationOwner](AlterPublicationOwner.md)
  - [AlterPublicationOwner_oid](AlterPublicationOwner_oid.md)

## Notes and Other Information
- This is a static function, only accessible within the publicationcmds.c compilation unit
- Returns early if the new owner is the same as the current owner (no-op optimization)
- Enforces that FOR ALL TABLES publications can only be owned by superusers
- Enforces that FOR TABLES IN SCHEMA publications can only be owned by superusers
- Requires the new owner to have CREATE privilege on the database
- Uses changeDependencyOnOwner to maintain proper dependency tracking in pg_depend
- Triggers post-alter hooks for proper event notification
- All permission checks are bypassed for superusers (current user), but restrictions on new owner still apply
- Updates the publication tuple in place and ensures catalog consistency
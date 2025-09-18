# AlterCollation

## Location
src/backend/commands/collationcmds.c: 428 - 510

## Overview
AlterCollation implements the ALTER COLLATION REFRESH VERSION command, updating a collation's version information to reflect changes in the underlying locale library.

## Definition


## Detailed Description
This function handles the ALTER COLLATION REFRESH VERSION SQL command by:
1. Validating that the target collation exists and the user has ownership privileges
2. Preventing alteration of the default collation (suggesting ALTER DATABASE instead)
3. Retrieving the current version information from the system catalog
4. Obtaining the actual current version from the collation provider
5. Comparing versions and updating the catalog entry if they differ
6. Providing user feedback through NOTICE messages about version changes

The function ensures version consistency between PostgreSQL's catalog and the underlying collation library, which is important for detecting potential collation behavior changes that could affect index integrity.

## Parameters / Member Variables
- : AlterCollationStmt structure containing the collation name to refresh

## Dependencies
- Functions called/Symbols referenced:
  - get_collation_oid
  - object_ownercheck
  - aclcheck_error
  - NameListToString
  - get_collation_actual_version
  - heap_modify_tuple
  - CatalogTupleUpdate
  - InvokeObjectPostAlterHook
- Called from (representative examples):
  - ProcessUtilitySlow

## Notes and Other Information
- Only supports REFRESH VERSION operation (other ALTER COLLATION operations are handled by generic alter functions)
- Prevents modification of DEFAULT_COLLATION_OID with helpful hint to use ALTER DATABASE instead
- Handles both libc and ICU collation providers by checking collForm->collprovider
- Validates that version changes are logical (cannot change from NULL to non-NULL or vice versa)
- Provides user-friendly NOTICE messages indicating whether the version changed or remained the same
- Uses heap_modify_tuple for atomic catalog updates and triggers appropriate post-alter hooks
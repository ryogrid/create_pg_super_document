# performDeletion

## Location
[src/backend/catalog/dependency.c:273-331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/dependency.c#L273-L331)

## Overview
The main control routine for deleting database objects and their dependencies, supporting both CASCADE and RESTRICT behaviors with various deletion flags.

## Definition


## Detailed Description
performDeletion is the primary entry point for dropping database objects that participate in PostgreSQL's dependency system. This function orchestrates the complete deletion process in several well-defined phases:

1. **Initialization**: Opens the pg_depend system catalog and acquires necessary locks
2. **Dependency Analysis**: Calls findDependentObjects to build a complete list of objects that need to be deleted
3. **Validation**: Uses reportDependentObjects to check permissions and report what will be deleted
4. **Execution**: Calls deleteObjectsInList to perform the actual deletions
5. **Cleanup**: Releases resources and closes catalog relations

The function supports both CASCADE behavior (delete dependent objects) and RESTRICT behavior (error if dependents exist). It handles various deletion scenarios through flag parameters, including internal operations, concurrent deletions, and extension-aware deletions.

## Parameters / Member Variables
- : Pointer to ObjectAddress identifying the primary object to be deleted
- : DropBehavior enum value (CASCADE or RESTRICT) controlling how dependencies are handled
- : Integer bitmask controlling deletion behavior with various PERFORM_DELETION_* flags:
  - PERFORM_DELETION_INTERNAL: Suppresses event triggers and some permission checks
  - PERFORM_DELETION_CONCURRENTLY: Performs concurrent deletion (mainly for indexes)
  - PERFORM_DELETION_QUIETLY: Reduces message verbosity
  - PERFORM_DELETION_SKIP_ORIGINAL: Deletes dependencies but not the original object
  - PERFORM_DELETION_SKIP_EXTENSIONS: Preserves extensions during deletion
  - PERFORM_DELETION_CONCURRENT_LOCK: Uses concurrent locking mode

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - [AcquireDeletionLock](../A/AcquireDeletionLock.md)
  - [new_object_addresses](../n/new_object_addresses.md)
  - [findDependentObjects](../f/findDependentObjects.md)
  - [reportDependentObjects](../r/reportDependentObjects.md)
  - [deleteObjectsInList](../d/deleteObjectsInList.md)
  - [free_object_addresses](../f/free_object_addresses.md)
  - table_close
- Data structures used:
  - [ObjectAddress](../O/ObjectAddress.md)
  - ObjectAddresses
  - DropBehavior
  - DEPFLAG_ORIGINAL
- Called from (representative examples):
  - [SetDefaultACL](../S/SetDefaultACL.md)
  - [RemoveTempRelations](../R/RemoveTempRelations.md)
  - [ATExecDropConstraint](../A/ATExecDropConstraint.md)
  - [DetachPartitionFinalize](../D/DetachPartitionFinalize.md)
  - [do_autovacuum](../d/do_autovacuum.md)

## Notes and Other Information
- This is a public function accessible throughout the PostgreSQL backend
- Opens pg_depend with RowExclusiveLock to ensure consistency during dependency analysis
- Acquires deletion lock on target object to prevent concurrent modifications
- The function is designed to be the single point of control for all DROP operations
- Related function performMultipleDeletions provides similar functionality for multiple objects
- Error handling and transaction management are handled by the calling context
- The dependency analysis phase can be computationally expensive for objects with many dependencies
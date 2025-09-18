# performMultipleDeletions

## Location
src/backend/catalog/dependency.c: 332 - 431

## Overview
A variant of performDeletion that efficiently handles deletion of multiple database objects simultaneously, with optimized dependency analysis and unified reporting.

## Definition


## Detailed Description
performMultipleDeletions extends the functionality of performDeletion to handle multiple objects in a single operation. The key advantage over multiple individual performDeletion calls is that it performs dependency analysis with the entire set of objects as context, which allows for more relaxed validation rules.

The function operates similarly to performDeletion but with important differences:

1. **Unified Context**: All objects to be deleted are considered together during dependency analysis, preventing failures when objects have internal dependencies on each other
2. **Optimized Processing**: Opens pg_depend once for all operations, reducing catalog access overhead  
3. **Batch Reporting**: Provides unified reporting of cascaded deletions across all objects
4. **Lock Acquisition**: Acquires deletion locks on all target objects before proceeding

The dependency analysis phase passes the complete objects list as pendingObjects context to findDependentObjects, which prevents internal dependency conflicts between objects in the same batch.

## Parameters / Member Variables
- : Pointer to ObjectAddresses structure containing all objects to be deleted
- : DropBehavior enum value (CASCADE or RESTRICT) controlling dependency handling behavior
- : Integer bitmask controlling deletion behavior, same flags as performDeletion including:
  - PERFORM_DELETION_INTERNAL: Suppresses event triggers and permission checks
  - PERFORM_DELETION_CONCURRENTLY: Performs concurrent deletion operations
  - PERFORM_DELETION_QUIETLY: Reduces message verbosity to DEBUG2 level
  - PERFORM_DELETION_SKIP_ORIGINAL: Deletes dependencies but preserves original objects
  - PERFORM_DELETION_SKIP_EXTENSIONS: Avoids deleting extension objects

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - new_object_addresses
  - AcquireDeletionLock
  - findDependentObjects
  - reportDependentObjects
  - deleteObjectsInList
  - free_object_addresses
  - table_close
- Data structures used:
  - ObjectAddresses
  - ObjectAddress
  - DropBehavior
  - DEPFLAG_ORIGINAL
- Called from (representative examples):
  - shdepDropOwned
  - RemoveObjects
  - RemoveRelations
  - ATExecDropColumn
  - tryAttachPartitionForeignKey

## Notes and Other Information
- This is a public function accessible throughout the PostgreSQL backend
- Returns early if the objects list is empty (numrefs <= 0)
- More efficient than multiple performDeletion calls when deleting related objects
- The unified context approach prevents cascading dependency errors between objects in the same batch
- Reporting is adapted based on whether there's exactly one object (detailed) or multiple objects (general)
- Uses the same locking and cleanup patterns as performDeletion
- Particularly useful for operations like DROP SCHEMA CASCADE where many related objects need deletion
- The pendingObjects parameter in findDependentObjects enables the key optimization that distinguishes this from repeated single deletions
# findDependentObjects

## Location
src/backend/catalog/dependency.c: 432 - 979

## Overview
Core recursive function that builds a complete dependency tree for object deletion, ensuring safe deletion order and handling complex dependency relationships including ownership, extensions, and partitioning.

## Definition


## Detailed Description
findDependentObjects is the heart of PostgreSQL's dependency analysis system. This complex recursive function performs comprehensive dependency traversal to determine all objects that must be deleted when dropping a given object. The function operates in several distinct phases:

**Phase 1 - Cycle Detection and Early Exits**: Checks for circular dependencies using a recursion stack, handles already-processed objects, and validates that pinned system objects cannot be dropped.

**Phase 2 - Ownership Analysis**: Scans pg_depend to identify objects that the current object depends on, particularly focusing on INTERNAL and EXTENSION dependencies that indicate ownership relationships. If the object is owned by another object, the function redirects to delete the owner first.

**Phase 3 - Dependent Object Collection**: Performs a reverse scan of pg_depend to find all objects that depend on the current object. These dependent objects are collected, sorted for consistent output, and then recursively processed.

**Phase 4 - Target Addition**: Finally adds the current object to the targetObjects list with appropriate flags and dependency metadata.

The function handles various dependency types (NORMAL, AUTO, INTERNAL, EXTENSION, PARTITION) with different behaviors for each. It ensures deletion order safety by processing dependencies before their dependents and includes sophisticated locking mechanisms to handle concurrent operations.

## Parameters / Member Variables
- : Pointer to ObjectAddress identifying the object to analyze for dependencies
- : Integer flags describing the reason for visiting this object (DEPFLAG_ORIGINAL, DEPFLAG_NORMAL, etc.)
- : Integer bitmask of PERFORM_DELETION_* flags controlling overall deletion behavior
- : Pointer to ObjectAddressStack for tracking recursion levels and detecting circular dependencies
- : Pointer to ObjectAddresses list where objects scheduled for deletion are accumulated
- : Pointer to const ObjectAddresses list of other objects being deleted (used in multiple deletions)
- : Pointer to already-opened pg_depend relation for dependency queries

## Dependencies
- Functions called/Symbols referenced:
  - stack_address_present_add_flags
  - check_stack_depth
  - object_address_present_add_flags
  - IsPinnedObject
  - getObjectDescription
  - systable_beginscan/systable_getnext/systable_endscan
  - systable_recheck_tuple
  - AcquireDeletionLock/ReleaseDeletionLock
  - object_address_present
  - qsort with object_address_comparator
  - add_exact_object_address_extra
- Data structures used:
  - ObjectAddress/ObjectAddresses
  - ObjectAddressStack/ObjectAddressAndFlags
  - ObjectAddressExtra
  - Form_pg_depend
  - Various DEPENDENCY_* and DEPFLAG_* constants
- Called from (representative examples):
  - performDeletion
  - performMultipleDeletions
  - findDependentObjects (recursive)
  - find_expr_references_context

## Notes and Other Information
- This is a static function, only accessible within the dependency.c module
- The function is inherently recursive and includes stack depth checking to prevent overflow
- Uses sophisticated locking protocols to handle concurrent object deletions safely
- Dependency types have specific semantics: INTERNAL/EXTENSION require owner-first deletion, NORMAL/AUTO allow cascading deletion
- The function can redirect deletion requests when ownership dependencies are encountered
- Partition dependencies (PARTITION_PRI/PARTITION_SEC) are handled specially for partitioned tables
- Object flags accumulate through recursion to track the path by which each object was reached
- The sorting of dependent objects ensures predictable deletion order for consistent behavior
- Early exit mechanisms optimize performance by avoiding duplicate work on already-processed objects
- The pendingObjects parameter enables batch deletion optimizations in performMultipleDeletions
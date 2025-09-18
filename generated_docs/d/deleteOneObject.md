# deleteOneObject

## Location
src/backend/catalog/dependency.c: 1246 - 1351

## Overview
deleteOneObject is a static function that performs the complete deletion of a single database object, including the object itself, its dependency records, and associated metadata like comments and security labels.

## Definition
```c
static void deleteOneObject(const ObjectAddress *object, Relation *depRel, int flags)
```

## Detailed Description
deleteOneObject orchestrates the complete removal of a database object by performing multiple cleanup operations in a specific order. The function first invokes drop hooks, then calls doDeletion to remove the object itself, followed by cleanup of dependency records in pg_depend, shared dependencies, and associated metadata (comments, security labels, initial privileges). For concurrent deletions, it manages the pg_depend relation lifecycle carefully to ensure transaction consistency. The function handles both complete object deletion (subId = 0) and sub-object deletion, ensuring all related records are properly removed.

## Parameters / Member Variables
- `object`: Pointer to ObjectAddress specifying the object to delete
  - `classId`: OID of the catalog relation containing the object
  - `objectId`: OID of the specific object to be deleted  
  - `objectSubId`: Sub-object identifier (0 for whole object deletion)
- `depRel`: Pointer to already-open pg_depend relation for dependency management
- `flags`: Deletion behavior flags including PERFORM_DELETION_CONCURRENTLY

## Dependencies
- Functions called/Symbols referenced:
  - InvokeObjectDropHookArg: Invokes drop hooks for the object
  - table_close: Closes relation for concurrent operations
  - [doDeletion](doDeletion.md): Performs object-specific deletion logic
  - table_open: Reopens relation after concurrent deletion
  - [ScanKeyInit](../S/ScanKeyInit.md): Initializes scan keys for dependency lookup
  - [systable_beginscan](../s/systable_beginscan.md): Begins scan of pg_depend relation
  - [systable_getnext](../s/systable_getnext.md): Gets next dependency record
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md): Deletes dependency records
  - [systable_endscan](../s/systable_endscan.md): Ends dependency scan
  - [deleteSharedDependencyRecordsFor](deleteSharedDependencyRecordsFor.md): Removes shared dependency records
  - [DeleteComments](../D/DeleteComments.md): Removes associated comments
  - [DeleteSecurityLabel](../D/DeleteSecurityLabel.md): Removes security labels
  - [DeleteInitPrivs](../D/DeleteInitPrivs.md): Removes initial privileges
  - CommandCounterIncrement: Ensures visibility of changes
- Called from:
  - find_expr_references_context: Expression reference finding context
  - [deleteObjectsInList](deleteObjectsInList.md): Bulk object deletion function

## Notes and Other Information
- This function is static and used internally within the dependency management system
- Handles concurrent deletion by carefully managing relation lifecycle and transaction boundaries
- Supports both complete object deletion and sub-object deletion based on objectSubId value
- Performs comprehensive cleanup including dependency records, shared dependencies, and metadata
- Uses CommandCounterIncrement to ensure all changes are visible for subsequent operations
- The order of operations is critical - object deletion occurs before dependency cleanup to handle concurrent cases properly
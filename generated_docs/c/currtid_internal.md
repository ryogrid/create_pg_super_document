# currtid_internal

## Location
src/backend/utils/adt/tid.c: 296 - 335

## Overview
A utility wrapper function that returns the latest version of a tuple pointing at a specified tuple identifier (TID) for a given relation, with proper access control checks.

## Definition
```c
static ItemPointer currtid_internal(Relation rel, ItemPointer tid)
```

## Detailed Description
The `currtid_internal` function serves as an internal utility wrapper for current CTID (Current Tuple Identifier) operations. It retrieves the latest version of a tuple identified by the given TID within the specified relation. The function performs comprehensive access control checks to ensure the user has SELECT privileges on the relation before proceeding with the operation.

The function handles different relation types appropriately:
- For views (RELKIND_VIEW), it delegates to `currtid_for_view`
- For relations without storage, it raises an error
- For regular tables with storage, it performs a table scan to get the latest TID

The implementation uses a snapshot-based approach to ensure consistent reads and properly manages the scan lifecycle with registration and cleanup of snapshots.

## Parameters / Member Variables
- `rel`: The relation (table/view) containing the tuple
- `tid`: Pointer to the tuple identifier for which to find the latest version

## Dependencies
- Functions called/Symbols referenced:
  - palloc
  - pg_class_aclcheck
  - aclcheck_error
  - get_relkind_objtype
  - currtid_for_view
  - get_namespace_name
  - ItemPointerCopy
  - GetLatestSnapshot
  - RegisterSnapshot
  - table_beginscan_tid
  - table_tuple_get_latest_tid
  - table_endscan
  - UnregisterSnapshot
- Called from (representative examples):
  - currtid_for_view
  - currtid_byrelname

## Notes and Other Information
- This is a static function, meaning it has internal linkage and is only accessible within the same translation unit
- The function allocates memory for the result ItemPointer using palloc, which is PostgreSQL's memory allocation function
- Access control is enforced at the relation level using ACL_SELECT permission
- The function properly handles different relation kinds and provides appropriate error messages for unsupported operations
- Snapshot management ensures MVCC (Multi-Version Concurrency Control) compliance during the TID lookup operation
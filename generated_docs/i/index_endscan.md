# index_endscan

## Location
[src/backend/access/index/indexam.c:378-407](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/indexam.c#L378-L407)

## Overview
The index_endscan function terminates an index scan operation and properly cleans up all resources associated with the scan, including buffer pins, reference counts, and the scan data structure itself.

## Definition

```c
structure itself */
	IndexScanEnd(scan);
```
## Detailed Description
index_endscan is responsible for the proper termination of an index scan operation. It performs a series of cleanup operations in a specific order to ensure that all resources are properly released. The function first releases any table access resources (like buffer pins from heap fetches), then calls the access method-specific end scan routine, decrements the index relation reference count, unregisters any temporary snapshots, and finally deallocates the scan descriptor itself.

The function includes validation checks through SCAN_CHECKS and CHECK_SCAN_PROCEDURE macros to ensure the scan descriptor is valid and the access method provides the required amendscan procedure.

## Parameters / Member Variables
- : IndexScanDesc - The index scan descriptor to be terminated and cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - SCAN_CHECKS (validation macro)
  - CHECK_SCAN_PROCEDURE (validation macro for amendscan)
  - [table_index_fetch_end](../t/table_index_fetch_end.md) (releases heap fetch resources)
  - [RelationDecrementReferenceCount](../R/RelationDecrementReferenceCount.md) (decrements index relation refcount)
  - [UnregisterSnapshot](../U/UnregisterSnapshot.md) (unregisters temporary snapshots)
  - [IndexScanEnd](../I/IndexScanEnd.md) (deallocates scan descriptor)
- Called from (representative examples):
  - [systable_endscan](../s/systable_endscan.md)
  - [ExecEndBitmapIndexScan](../E/ExecEndBitmapIndexScan.md)
  - [ExecEndIndexOnlyScan](../E/ExecEndIndexOnlyScan.md)
  - [ExecEndIndexScan](../E/ExecEndIndexScan.md)
  - [check_exclusion_or_unique_constraint](../c/check_exclusion_or_unique_constraint.md)

## Notes and Other Information
- This function must be called to properly terminate any index scan started with index_beginscan
- The function handles both regular index scans and those with heap fetch operations
- Resource cleanup is performed in a specific order: heap fetch resources first, then AM-specific cleanup, then reference count management, snapshot cleanup, and finally scan descriptor deallocation
- The function is part of the index access method interface in PostgreSQL's storage system
- Located in src/backend/access/index/indexam.c:378-407

## Simplified Source

```c
void
index_endscan(IndexScanDesc scan)
{
    // Validate scan descriptor and access method
    SCAN_CHECKS;
    CHECK_SCAN_PROCEDURE(amendscan);

    // Clean up table access resources (buffer pins, etc.)
    if (scan->xs_heapfetch)
    {
        table_index_fetch_end(scan->xs_heapfetch);
        scan->xs_heapfetch = NULL;
    }

    // Call access method-specific end scan routine
    scan->indexRelation->rd_indam->amendscan(scan);

    // Release index relation reference count
    RelationDecrementReferenceCount(scan->indexRelation);

    // Unregister temporary snapshot if used
    if (scan->xs_temp_snap)
        UnregisterSnapshot(scan->xs_snapshot);

    // Release the scan descriptor itself
    IndexScanEnd(scan);
}
```
# index_beginscan_bitmap

## Location
[src/backend/access/index/indexam.c:287-309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/indexam.c#L287-L309)

## Overview
The  function initiates a bitmap index scan operation using the amgetbitmap access method, which is optimized for bulk retrieval of matching tuple identifiers.

## Definition

```c
IndexScanDesc
index_beginscan_bitmap(Relation indexRelation,
					   Snapshot snapshot,
					   int nkeys)
```
## Detailed Description
This function creates and initializes an index scan descriptor specifically for bitmap scanning operations. Unlike regular index scans that retrieve tuples one by one, bitmap scans are designed to collect all matching tuple identifiers (TIDs) at once and return them as a bitmap. The function internally calls  with specific parameters optimized for bitmap operations (norderbys=0 since bitmap scans don't preserve order). It's simpler than  because bitmap scans don't need heap tuple fetching setup - that's handled separately when the bitmap is later used to fetch actual tuples.

## Parameters / Member Variables
- : The index relation to be scanned for bitmap creation
- : The snapshot to use for visibility checking during the scan (must not be InvalidSnapshot)  
- : Number of scan keys (search conditions) for the scan

## Dependencies
- Functions called/Symbols referenced:
  - [index_beginscan_internal](index_beginscan_internal.md) (internal scan initialization with bitmap-specific parameters)
  - InvalidSnapshot (constant for validation)
  - [IndexScanDesc](../I/IndexScanDesc.md) (return type structure)
- Called from (representative examples):
  - [ExecInitBitmapIndexScan](../E/ExecInitBitmapIndexScan.md) (src/backend/executor/nodeBitmapIndexscan.c:303)

## Notes and Other Information
- Specifically designed for bitmap index scan operations in PostgreSQL's query execution
- Does not set up heap tuple fetching like regular index scans since bitmap scans work differently
- The norderbys parameter is hardcoded to 0 because bitmap scans don't preserve tuple ordering
- Caller must hold appropriate locks on the parent heap relation (though not explicitly passed)
- Part of PostgreSQL's bitmap scan optimization for OR conditions and bulk operations
- Located in src/backend/access/index/indexam.c:287-309

## Simplified Source

```c
IndexScanDesc
index_beginscan_bitmap(Relation indexRelation,
                       Snapshot snapshot,
                       int nkeys)
{
    IndexScanDesc scan;

    // Validate snapshot
    Assert(snapshot != InvalidSnapshot);

    // Initialize scan descriptor for bitmap operations
    // norderbys=0 because bitmap scans don't preserve order
    scan = index_beginscan_internal(indexRelation, nkeys, 0, snapshot, NULL, false);

    // Store snapshot in scan descriptor
    scan->xs_snapshot = snapshot;

    return scan;
}
```
# index_beginscan

## Location
[src/backend/access/index/indexam.c:256-286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/indexam.c#L256-L286)

## Overview
The  function initiates a new index scan operation using the amgettuple access method, setting up the necessary structures for tuple-by-tuple index traversal.

## Definition

```c
IndexScanDesc
index_beginscan(Relation heapRelation,
				Relation indexRelation,
				Snapshot snapshot,
				int nkeys, int norderbys)
```
## Detailed Description
This function creates and initializes an index scan descriptor for scanning an index with tuple-level access. It serves as a high-level interface that internally calls  and then sets up additional parameters specific to heap relation access. The function prepares the scan for fetching index matches from the underlying table by initializing the heap fetch mechanism. The caller must hold appropriate locks on both the heap relation and index relation before calling this function.

## Parameters / Member Variables
- `heapRelation`: The heap relation associated with the index being scanned
- `indexRelation`: The index relation to be scanned
- `snapshot`: The snapshot to use for visibility checking during the scan (must not be InvalidSnapshot)
- `nkeys`: Number of scan keys (search conditions) for the scan
- `norderbys`: Number of ordering specifications for the scan
## Dependencies
- Functions called/Symbols referenced:
  - [index_beginscan_internal](index_beginscan_internal.md) (internal scan initialization)
  - [table_index_fetch_begin](../t/table_index_fetch_begin.md) (heap fetch preparation)
  - InvalidSnapshot (constant for validation)
  - [IndexScanDesc](../I/IndexScanDesc.md) (return type structure)
- Called from (representative examples):
  - [systable_beginscan](../s/systable_beginscan.md) (src/backend/access/index/genam.c:442)
  - [systable_beginscan_ordered](../s/systable_beginscan_ordered.md) (src/backend/access/index/genam.c:700)
  - [IndexNext](../I/IndexNext.md) (src/backend/executor/nodeIndexscan.c:109)
  - [IndexOnlyNext](../I/IndexOnlyNext.md) (src/backend/executor/nodeIndexonlyscan.c:92)

## Notes and Other Information
- Part of PostgreSQL's index access method abstraction layer
- Requires valid snapshot parameter - assertion will fail if InvalidSnapshot is passed
- The returned IndexScanDesc must be properly closed with index_endscan when scan is complete
- Sets up both index scanning and heap tuple fetching mechanisms
- Located in src/backend/access/index/indexam.c:256-286

## Simplified Source

```c
// Simplified version of index_beginscan
IndexScanDesc
index_beginscan(Relation heapRelation,
                Relation indexRelation,
                Snapshot snapshot,
                int nkeys, int norderbys)
{
    IndexScanDesc scan;

    // Validate snapshot parameter
    Assert(snapshot != InvalidSnapshot);

    // Initialize the index scan descriptor with internal helper
    scan = index_beginscan_internal(indexRelation, nkeys, norderbys,
                                   snapshot, NULL, false);

    // Set up heap relation and snapshot in scan descriptor
    scan->heapRelation = heapRelation;
    scan->xs_snapshot = snapshot;

    // Prepare heap tuple fetching mechanism for index matches
    scan->xs_heapfetch = table_index_fetch_begin(heapRelation);

    return scan;
}
```

Key simplifications made:
- Preserved the essential initialization flow and parameter assignments
- Kept the critical snapshot validation assertion
- Maintained the core three-step process: internal scan setup, parameter assignment, and heap fetch preparation
- Added descriptive comments for each major step
- Preserved all function parameters and return type for interface clarity
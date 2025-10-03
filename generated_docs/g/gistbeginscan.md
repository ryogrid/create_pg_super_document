# gistbeginscan

## Location
[src/backend/access/gist/gistscan.c:74-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistscan.c#L74-L126)

## Overview
Initializes and begins a scan operation on a GiST (Generalized Search Tree) index, setting up all necessary data structures and memory contexts for the scan lifecycle.

## Definition

```c
IndexScanDesc
gistbeginscan(Relation r, int nkeys, int norderbys)
```
## Detailed Description
This function serves as the entry point for GiST index scanning operations, implementing the index access method API for PostgreSQL. It creates and initializes an IndexScanDesc structure along with GiST-specific opaque data (GISTScanOpaque) that contains all the state information needed throughout the scan's lifetime. The function establishes memory contexts for efficient memory management, sets up distance tracking arrays for ORDER BY operations, and prepares workspace for scan keys. All memory allocations are performed in the scan-lifetime context to ensure automatic cleanup when the scan ends.

The function follows PostgreSQL's index AM (Access Method) interface, making it pluggable into the query executor's index scanning framework. It defers some initialization work to gistrescan() since the exact scan parameters may not be known until the actual scan begins.

## Parameters / Member Variables
- : The relation (table/index) being scanned
- : Number of scan keys (WHERE clause conditions)
- : Number of ORDER BY expressions for nearest-neighbor queries

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetIndexScan](../R/RelationGetIndexScan.md)
  - [initGISTstate](../i/initGISTstate.md)
  - [createTempGistContext](../c/createTempGistContext.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc0](../p/palloc0.md)
  - [palloc](../p/palloc.md)
  - memset
- Called from (representative examples):
  - [gisthandler](gisthandler.md)

## Notes and Other Information
- This function only initializes the scan structure; actual scanning begins with gistrescan()
- Memory management uses hierarchical contexts: scanCxt contains all scan-lifetime data
- Supports both regular index scans and index-only scans (though index-only scan fields are initialized in gistrescan)
- The killed items tracking mechanism is initialized but not allocated until needed
- Part of PostgreSQL's pluggable index access method architecture

## Simplified Source

```c
IndexScanDesc gistbeginscan(Relation r, int nkeys, int norderbys) {
    IndexScanDesc scan;
    GISTSTATE *giststate;
    GISTScanOpaque so;
    MemoryContext oldCxt;

    // Initialize basic scan descriptor
    scan = RelationGetIndexScan(r, nkeys, norderbys);

    // Set up GIST state with scan-lifetime memory context
    giststate = initGISTstate(scan->indexRelation);

    // Switch to scan context for memory management
    oldCxt = MemoryContextSwitchTo(giststate->scanCxt);

    // Initialize GiST-specific opaque data
    so = (GISTScanOpaque) palloc0(sizeof(GISTScanOpaqueData));
    so->giststate = giststate;
    giststate->tempCxt = createTempGistContext();
    so->queue = NULL;
    so->queueCxt = giststate->scanCxt;

    // Set up distance tracking for ORDER BY operations
    so->distances = palloc(sizeof(so->distances[0]) * scan->numberOfOrderBys);
    so->qual_ok = true;

    if (scan->numberOfOrderBys > 0) {
        scan->xs_orderbyvals = palloc0(sizeof(Datum) * scan->numberOfOrderBys);
        scan->xs_orderbynulls = palloc(sizeof(bool) * scan->numberOfOrderBys);
        memset(scan->xs_orderbynulls, true, sizeof(bool) * scan->numberOfOrderBys);
    }

    // Initialize scan state tracking
    so->killedItems = NULL;
    so->numKilled = 0;
    so->curBlkno = InvalidBlockNumber;
    so->curPageLSN = InvalidXLogRecPtr;

    scan->opaque = so;

    // Restore previous memory context
    MemoryContextSwitchTo(oldCxt);

    return scan;
}
```
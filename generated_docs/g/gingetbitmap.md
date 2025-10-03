# gingetbitmap

## Location
[src/backend/access/gin/ginget.c:1918-1969](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginget.c#L1918-L1969)

## Overview
The  function is the core bitmap scan implementation for GIN (Generalized Inverted Index) indexes, responsible for collecting all matching tuple IDs into a TIDBitmap during index scans.

## Definition

```c
int64
gingetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
```
## Detailed Description
This function performs a complete bitmap scan of a GIN index, collecting all tuples that satisfy the scan conditions into the provided TIDBitmap. The function implements a two-phase scanning strategy: first scanning the pending list for recently inserted items that haven't been integrated into the main index structure, then scanning the main index itself.

The function handles concurrent access considerations by ensuring the pending list is scanned before the main index to prevent missing entries due to concurrent cleanup operations. It supports both exact tuple references and lossy page references for efficient bitmap storage.

## Parameters / Member Variables
- : An IndexScanDesc structure containing the scan state and conditions for the GIN index scan
- : A TIDBitmap structure where matching tuple IDs and page references will be collected

## Dependencies
- Functions called/Symbols referenced:
  - [ginFreeScanKeys](ginFreeScanKeys.md)
  - [ginNewScanKey](ginNewScanKey.md)
  - GinIsVoidRes
  - [scanPendingInsert](../s/scanPendingInsert.md)
  - [startScan](../s/startScan.md)
  - ItemPointerSetMin
  - [scanGetItem](../s/scanGetItem.md)
  - ItemPointerIsLossyPage
  - [tbm_add_page](../t/tbm_add_page.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [tbm_add_tuples](../t/tbm_add_tuples.md)
- Called from (representative examples):
  - [ginhandler](ginhandler.md) (GIN access method handler)
  - Referenced in GinScanOpaque structure

## Notes and Other Information
- The function returns the total number of tuples/pages added to the bitmap
- Supports lossy page-level bitmap entries when individual tuple tracking becomes inefficient
- The two-phase scan (pending list first, then main index) is critical for correctness in concurrent environments
- Duplicate visits to the same tuple are harmless as they just re-set the same bit in the bitmap
- This dual-scanning approach is one reason why GIN indexes cannot support the amgettuple API (tuple-at-a-time retrieval)
- The function handles void (unsatisfiable) query conditions by returning early with zero results

## Simplified Source
```c
int64 gingetbitmap(IndexScanDesc scan, TIDBitmap *tbm) {
    GinScanOpaque so = (GinScanOpaque) scan->opaque;
    int64 ntids;
    ItemPointerData iptr;
    bool recheck;

    // Set up scan keys and check for unsatisfiable query
    ginFreeScanKeys(so);
    ginNewScanKey(scan);

    if (GinIsVoidRes(scan))
        return 0;

    ntids = 0;

    // Phase 1: Scan pending list first to handle recent inserts
    // This must come before main index scan to prevent missing entries
    // due to concurrent cleanup operations
    scanPendingInsert(scan, tbm, &ntids);

    // Phase 2: Scan main index structure
    startScan(scan);
    ItemPointerSetMin(&iptr);

    for (;;) {
        if (!scanGetItem(scan, iptr, &iptr, &recheck))
            break;

        // Add to bitmap - either individual tuple or entire page
        if (ItemPointerIsLossyPage(&iptr))
            tbm_add_page(tbm, ItemPointerGetBlockNumber(&iptr));
        else
            tbm_add_tuples(tbm, &iptr, 1, recheck);

        ntids++;
    }

    return ntids;
}
```
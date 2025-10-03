# gistrescan

## Location
[src/backend/access/gist/gistscan.c:127-348](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistscan.c#L127-L348)

## Overview
Reinitializes or restarts a GiST index scan with potentially new scan keys and order-by conditions, managing memory contexts and preparing the search queue for traversal.

## Definition

```c
struct a descriptor with the original data
		 * types.
		 */
		natts = RelationGetNumberOfAttributes(scan->indexRelation);
```
## Detailed Description
This function handles the reinitialization of an existing GiST index scan, which can occur either as the initial scan setup (called after gistbeginscan) or when restarting a scan with different parameters. It implements sophisticated memory management using multiple contexts to optimize for the common case of single rescans while handling multiple rescans efficiently. The function processes scan keys by replacing operator functions with consistent functions, handles ORDER BY clauses by setting up distance functions, and prepares index-only scan infrastructure when needed.

The function creates a pairing heap-based priority queue for organizing search items, properly handling memory context switches to ensure all allocations are in the correct lifetime scope. It also validates scan keys for NULL handling and sets up function caching mechanisms to preserve performance across multiple scans.

## Parameters / Member Variables
- : The IndexScanDesc structure representing the ongoing index scan
- : Array of new scan key conditions (WHERE clause predicates)
- : Number of scan keys (ignored, uses scan->numberOfKeys instead)
- : Array of ORDER BY expressions for distance-based queries
- : Number of ORDER BY expressions (ignored, uses scan->numberOfOrderBys instead)

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - RelationGetNumberOfAttributes
  - IndexRelationGetNumberOfKeyAttributes
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md)
  - [pairingheap_allocate](../p/pairingheap_allocate.md)
  - [pairingheap_GISTSearchItem_cmp](../p/pairingheap_GISTSearchItem_cmp.md)
  - [fmgr_info_copy](../f/fmgr_info_copy.md)
  - [get_func_rettype](get_func_rettype.md)
  - [palloc](../p/palloc.md)
  - memmove
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [gisthandler](gisthandler.md)

## Notes and Other Information
- Implements a three-tier memory context strategy: first scan uses scanCxt, second scan creates queueCxt, subsequent scans reset queueCxt
- Supports both regular scans and index-only scans with proper tuple descriptor setup
- Handles NULL scan keys according to SK_SEARCHNULL/SK_SEARCHNOTNULL flags
- Preserves function extra data (fn_extra) across rescans for performance
- Distance functions must return float8 regardless of the original ordering operator's return type
- The nkeys and norderbys parameters are ignored in favor of the counts stored in the IndexScanDesc structure

## Simplified Source

```c
void
gistrescan(IndexScanDesc scan, ScanKey key, int nkeys,
           ScanKey orderbys, int norderbys)
{
    GISTScanOpaque so = (GISTScanOpaque) scan->opaque;
    bool first_time;
    MemoryContext oldCxt;

    // Determine if this is first, second, or subsequent rescan
    if (so->queue == NULL) {
        // First time - use existing scanCxt
        first_time = true;
    } else if (so->queueCxt == so->giststate->scanCxt) {
        // Second time - create dedicated queue context
        so->queueCxt = AllocSetContextCreate(so->giststate->scanCxt,
                                           "GiST queue context",
                                           ALLOCSET_DEFAULT_SIZES);
        first_time = false;
    } else {
        // Third+ time - reset queue context
        MemoryContextReset(so->queueCxt);
        first_time = false;
    }

    // Initialize index-only scan tuple descriptor if needed
    if (scan->xs_want_itup && !scan->xs_hitupdesc) {
        int natts = RelationGetNumberOfAttributes(scan->indexRelation);
        int nkeyatts = IndexRelationGetNumberOfKeyAttributes(scan->indexRelation);

        // Create tuple descriptor with original data types
        so->giststate->fetchTupdesc = CreateTemplateTupleDesc(natts);

        // Set up key attributes
        for (int attno = 1; attno <= nkeyatts; attno++) {
            TupleDescInitEntry(so->giststate->fetchTupdesc, attno, NULL,
                             scan->indexRelation->rd_opcintype[attno - 1],
                             -1, 0);
        }

        // Set up non-key attributes
        for (int attno = nkeyatts + 1; attno <= natts; attno++) {
            TupleDescInitEntry(so->giststate->fetchTupdesc, attno, NULL,
                             TupleDescAttr(so->giststate->leafTupdesc,
                                         attno - 1)->atttypid,
                             -1, 0);
        }

        scan->xs_hitupdesc = so->giststate->fetchTupdesc;

        // Create page data context for returned tuples
        so->pageDataCxt = AllocSetContextCreate(so->giststate->scanCxt,
                                              "GiST page data context",
                                              ALLOCSET_DEFAULT_SIZES);
    }

    // Create new search queue (pairing heap)
    oldCxt = MemoryContextSwitchTo(so->queueCxt);
    so->queue = pairingheap_allocate(pairingheap_GISTSearchItem_cmp, scan);
    MemoryContextSwitchTo(oldCxt);

    so->firstCall = true;

    // Update scan keys if provided
    if (key && scan->numberOfKeys > 0) {
        void **fn_extras = NULL;

        // Preserve function extras on subsequent scans
        if (!first_time) {
            fn_extras = (void **) palloc(scan->numberOfKeys * sizeof(void *));
            for (int i = 0; i < scan->numberOfKeys; i++)
                fn_extras[i] = scan->keyData[i].sk_func.fn_extra;
        }

        // Copy new scan keys
        memmove(scan->keyData, key, scan->numberOfKeys * sizeof(ScanKeyData));

        so->qual_ok = true;

        // Replace operator functions with consistent functions
        for (int i = 0; i < scan->numberOfKeys; i++) {
            ScanKey skey = scan->keyData + i;

            fmgr_info_copy(&(skey->sk_func),
                          &(so->giststate->consistentFn[skey->sk_attno - 1]),
                          so->giststate->scanCxt);

            if (!first_time)
                skey->sk_func.fn_extra = fn_extras[i];

            // Check for NULL handling
            if (skey->sk_flags & SK_ISNULL) {
                if (!(skey->sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL)))
                    so->qual_ok = false;
            }
        }

        if (!first_time)
            pfree(fn_extras);
    }

    // Update ORDER BY keys if provided
    if (orderbys && scan->numberOfOrderBys > 0) {
        void **fn_extras = NULL;

        // Preserve function extras on subsequent scans
        if (!first_time) {
            fn_extras = (void **) palloc(scan->numberOfOrderBys * sizeof(void *));
            for (int i = 0; i < scan->numberOfOrderBys; i++)
                fn_extras[i] = scan->orderByData[i].sk_func.fn_extra;
        }

        // Copy new order-by keys
        memmove(scan->orderByData, orderbys,
                scan->numberOfOrderBys * sizeof(ScanKeyData));

        so->orderByTypes = (Oid *) palloc(scan->numberOfOrderBys * sizeof(Oid));

        // Replace operator functions with distance functions
        for (int i = 0; i < scan->numberOfOrderBys; i++) {
            ScanKey skey = scan->orderByData + i;
            FmgrInfo *finfo = &(so->giststate->distanceFn[skey->sk_attno - 1]);

            // Validate distance function exists
            if (!OidIsValid(finfo->fn_oid))
                elog(ERROR, "missing support function %d for attribute %d of index \"%s\"",
                     GIST_DISTANCE_PROC, skey->sk_attno,
                     RelationGetRelationName(scan->indexRelation));

            so->orderByTypes[i] = get_func_rettype(skey->sk_func.fn_oid);

            fmgr_info_copy(&(skey->sk_func), finfo, so->giststate->scanCxt);

            if (!first_time)
                skey->sk_func.fn_extra = fn_extras[i];
        }

        if (!first_time)
            pfree(fn_extras);
    }

    scan->xs_hitup = NULL;
}
```
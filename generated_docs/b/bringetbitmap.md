# bringetbitmap

## Location
[src/backend/access/brin/brin.c:558-947](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L558-L947)

## Overview
Executes a BRIN index scan and returns a bitmap of heap pages that match the scan keys by reading index tuples from the revmap and comparing their summary values against scan conditions.

## Definition

```c
int64
bringetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
```
## Detailed Description
The bringetbitmap function is the core bitmap index scan implementation for BRIN (Block Range Index) indexes. It works by:

1. Reading index TIDs from the revmap (reverse mapping) structure
2. Obtaining index tuples pointed to by these TIDs  
3. Comparing summary values in the index tuples to scan keys
4. Adding all pages in matching ranges to the TID bitmap

For ranges that are unsummarized (marked with InvalidTID in revmap), all pages in those ranges are returned regardless of scan keys since no summary information is available.

The function processes each page range by:
- Retrieving the BRIN tuple for the range from the revmap
- If no tuple exists (unsummarized range), adding all pages in the range
- If a tuple exists, deforming it and checking if it's a placeholder tuple
- For regular tuples, comparing each indexed attribute's summary values against corresponding scan keys
- Using the attribute's consistent support procedure to determine if the range matches
- Adding qualifying page ranges to the output bitmap

## Parameters / Member Variables
- `scan`: IndexScanDesc containing scan keys, index relation, and opaque scan state
- `*tbm`: TIDBitmap to populate with qualifying heap page numbers
## Dependencies
- Functions called/Symbols referenced:
  - [brinGetTupleForHeapBlock](brinGetTupleForHeapBlock.md): Retrieves BRIN tuple for a given heap block
  - [brin_deform_tuple](brin_deform_tuple.md): Converts physical tuple to in-memory format
  - [brin_copy_tuple](brin_copy_tuple.md): Creates a copy of a BRIN tuple
  - [check_null_keys](../c/check_null_keys.md): Validates IS NULL/IS NOT NULL scan conditions  
  - [index_getprocinfo](../i/index_getprocinfo.md): Gets consistent support procedure for attribute
  - [tbm_add_page](../t/tbm_add_page.md): Adds page to TID bitmap
  - pgstat_count_index_scan: Updates index scan statistics
- Called from (representative examples):
  - [brinhandler](brinhandler.md): BRIN access method handler registration

## Notes and Other Information
- Returns an approximate count of tuples (totalpages * 10) rather than exact tuple count
- Uses a per-range memory context that is reset for each range to avoid memory leaks  
- Supports both single-key and multi-key consistent functions based on function signature
- Handles both regular scan keys and IS NULL/IS NOT NULL conditions separately
- Processes scan keys by grouping them per indexed attribute for efficient evaluation
- Empty ranges (bt_empty_range = true) are automatically excluded from results

## Simplified Source

```c
int64 bringetbitmap(IndexScanDesc scan, TIDBitmap *tbm) {
    Relation idxRel = scan->indexRelation;
    BrinOpaque *opaque = (BrinOpaque *) scan->opaque;
    BrinDesc *bdesc = opaque->bo_bdesc;

    // Get heap relation size to know iteration bounds
    Oid heapOid = IndexGetRelation(RelationGetRelid(idxRel), false);
    Relation heapRel = table_open(heapOid, AccessShareLock);
    BlockNumber nblocks = RelationGetNumberOfBlocks(heapRel);
    table_close(heapRel, AccessShareLock);

    // Prepare consistent support functions for each indexed attribute
    FmgrInfo *consistentFn = palloc0_array(FmgrInfo, bdesc->bd_tupdesc->natts);

    // Organize scan keys by attribute for efficient processing
    ScanKey **keys, **nullkeys;
    int *nkeys, *nnullkeys;
    // ... allocate and populate key arrays per attribute ...

    // Preprocess scan keys - group by attribute
    for (int keyno = 0; keyno < scan->numberOfKeys; keyno++) {
        ScanKey key = &scan->keyData[keyno];
        AttrNumber keyattno = key->sk_attno;

        // Get consistent function for this attribute if first time
        if (consistentFn[keyattno - 1].fn_oid == InvalidOid) {
            FmgrInfo *tmp = index_getprocinfo(idxRel, keyattno, BRIN_PROCNUM_CONSISTENT);
            fmgr_info_copy(&consistentFn[keyattno - 1], tmp, CurrentMemoryContext);
        }

        // Categorize as null or regular key
        if (key->sk_flags & SK_ISNULL) {
            nullkeys[keyattno - 1][nnullkeys[keyattno - 1]++] = key;
        } else {
            keys[keyattno - 1][nkeys[keyattno - 1]++] = key;
        }
    }

    int64 totalpages = 0;
    BrinMemTuple *dtup = brin_new_memtuple(bdesc);

    // Scan each page range in the index
    for (BlockNumber heapBlk = 0; heapBlk < nblocks; heapBlk += opaque->bo_pagesPerRange) {
        bool addrange = false;

        // Get BRIN tuple for this page range
        BrinTuple *tup = brinGetTupleForHeapBlock(opaque->bo_rmAccess, heapBlk, &buf, &off, &size, BUFFER_LOCK_SHARE);

        if (!tup) {
            // No summary exists - must include entire range
            addrange = true;
        } else {
            // Check if summary matches scan keys
            dtup = brin_deform_tuple(bdesc, tup, dtup);

            if (dtup->bt_placeholder) {
                // Placeholder tuples always match
                addrange = true;
            } else {
                // Compare each indexed attribute against scan keys
                addrange = true; // Default to include unless excluded

                for (int attno = 1; attno <= bdesc->bd_tupdesc->natts; attno++) {
                    // Skip attributes with no scan keys
                    if (nkeys[attno - 1] == 0 && nnullkeys[attno - 1] == 0)
                        continue;

                    BrinValues *bval = &dtup->bt_columns[attno - 1];

                    // Empty ranges never match
                    if (dtup->bt_empty_range) {
                        addrange = false;
                        break;
                    }

                    // Check IS NULL/IS NOT NULL conditions
                    if (nnullkeys[attno - 1] > 0 &&
                        !check_null_keys(bval, nullkeys[attno - 1], nnullkeys[attno - 1])) {
                        addrange = false;
                        break;
                    }

                    // Check regular scan keys using consistent function
                    if (nkeys[attno - 1] > 0) {
                        if (bval->bv_allnulls) {
                            addrange = false;
                            break;
                        }

                        // Call consistent function to check if range matches
                        Datum result = FunctionCall4Coll(&consistentFn[attno - 1],
                                                       keys[attno - 1][0]->sk_collation,
                                                       PointerGetDatum(bdesc),
                                                       PointerGetDatum(bval),
                                                       PointerGetDatum(keys[attno - 1]),
                                                       Int32GetDatum(nkeys[attno - 1]));
                        addrange = DatumGetBool(result);

                        if (!addrange)
                            break;
                    }
                }
            }
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        }

        // Add all pages in matching ranges to bitmap
        if (addrange) {
            for (BlockNumber pageno = heapBlk;
                 pageno <= Min(nblocks, heapBlk + opaque->bo_pagesPerRange) - 1;
                 pageno++) {
                tbm_add_page(tbm, pageno);
                totalpages++;
            }
        }
    }

    // Return approximate tuple count
    return totalpages * 10;
}
```
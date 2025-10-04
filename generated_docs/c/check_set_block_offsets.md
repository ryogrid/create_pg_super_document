# check_set_block_offsets

## Location
[src/test/modules/test_tidstore/test_tidstore.c:220-313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_tidstore/test_tidstore.c#L220-L313)

## Overview
A comprehensive verification function that validates the correctness of TID (tuple identifier) storage operations by comparing stored TIDs against verification arrays using multiple access methods.

## Definition

```c
Datum
check_set_block_offsets(PG_FUNCTION_ARGS)
```
## Detailed Description
This function performs thorough validation of TidStore functionality by implementing a multi-stage verification process. It checks TID storage integrity through three different approaches: direct member lookup, comprehensive block scanning, and iteration-based retrieval. The function ensures that all three methods return consistent results and match the original verification data.

The verification process includes: (1) checking that all inserted TIDs are present using TidStoreIsMember, (2) performing exhaustive lookups across all possible offsets for each block to build a lookup array, (3) iterating through the TidStore to collect all stored TIDs, and (4) comparing all three datasets after sorting to ensure consistency.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [check_tidstore_available](check_tidstore_available.md) - Validates tidstore availability
  - [TidStoreIsMember](../T/TidStoreIsMember.md) - Checks if a TID exists in the store
  - [TidStoreLockShare](../T/TidStoreLockShare.md)/TidStoreUnlock - Shared locking for thread-safe read operations
  - [TidStoreBeginIterate](../T/TidStoreBeginIterate.md)/TidStoreIterateNext/TidStoreEndIterate - Iterator interface for TidStore
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)/ItemPointerGetOffsetNumber - Extract components from TIDs
  - [ItemPointerSet](../I/ItemPointerSet.md) - Construct TID values
  - qsort - [Sort](../S/Sort.md) arrays for comparison
  - [itemptr_cmp](../i/itemptr_cmp.md) - Custom comparison function for ItemPointer sorting
- Called from (representative examples):
  - No direct references found (likely called via SQL interface in tests)

## Notes and Other Information
- Located in src/test/modules/test_tidstore/test_tidstore.c:220-313
- Implements a rigorous three-way verification strategy to ensure TidStore correctness
- Uses shared locking during read operations to allow concurrent access while maintaining consistency
- Performs exhaustive scanning of all possible offsets (FirstOffsetNumber to MaxOffsetNumber) for thorough validation
- Validates that iteration results match both direct lookups and comprehensive scanning
- Raises detailed ERROR messages with specific TID information when mismatches are detected
- Essential component of the PostgreSQL TidStore testing framework, ensuring reliability of the storage mechanism
- Returns void as verification is the primary purpose rather than data retrieval

## Simplified Source

```c
Datum
check_set_block_offsets(PG_FUNCTION_ARGS)
{
    int num_iter_tids = 0;
    int num_lookup_tids = 0;
    BlockNumber prevblkno = 0;

    check_tidstore_available();

    // Verify all inserted TIDs are present in tidstore
    for (int i = 0; i < items.num_tids; i++)
        if (!TidStoreIsMember(tidstore, &items.insert_tids[i]))
            elog(ERROR, "missing TID with block %u, offset %u",
                 ItemPointerGetBlockNumber(&items.insert_tids[i]),
                 ItemPointerGetOffsetNumber(&items.insert_tids[i]));

    // Build lookup array by scanning all possible offsets for each block
    for (int i = 0; i < items.num_tids; i++) {
        BlockNumber blkno = ItemPointerGetBlockNumber(&items.insert_tids[i]);
        if (i > 0 && blkno == prevblkno) continue;

        for (OffsetNumber offset = FirstOffsetNumber; offset < MaxOffsetNumber; offset++) {
            ItemPointerData tid;
            ItemPointerSet(&tid, blkno, offset);

            TidStoreLockShare(tidstore);
            if (TidStoreIsMember(tidstore, &tid))
                ItemPointerSet(&items.lookup_tids[num_lookup_tids++], blkno, offset);
            TidStoreUnlock(tidstore);
        }
        prevblkno = blkno;
    }

    // Collect all TIDs through iteration
    TidStoreLockShare(tidstore);
    TidStoreIter *iter = TidStoreBeginIterate(tidstore);
    TidStoreIterResult *iter_result;
    while ((iter_result = TidStoreIterateNext(iter)) != NULL) {
        for (int i = 0; i < iter_result->num_offsets; i++)
            ItemPointerSet(&(items.iter_tids[num_iter_tids++]),
                          iter_result->blkno, iter_result->offsets[i]);
    }
    TidStoreEndIterate(iter);
    TidStoreUnlock(tidstore);

    // Verify all three methods found same number of TIDs
    if (num_lookup_tids != items.num_tids)
        elog(ERROR, "should have %d TIDs, have %d", items.num_tids, num_lookup_tids);
    if (num_iter_tids != items.num_tids)
        elog(ERROR, "should have %d TIDs, have %d", items.num_tids, num_iter_tids);

    // Sort and compare all arrays for consistency
    qsort(items.insert_tids, items.num_tids, sizeof(ItemPointerData), itemptr_cmp);
    qsort(items.lookup_tids, items.num_tids, sizeof(ItemPointerData), itemptr_cmp);
    for (int i = 0; i < items.num_tids; i++) {
        // Check iteration results match
        if (itemptr_cmp(&items.insert_tids[i], &items.iter_tids[i]) != 0)
            elog(ERROR, "TID iter array doesn't match verification array");
        // Check lookup results match
        if (itemptr_cmp(&items.insert_tids[i], &items.lookup_tids[i]) != 0)
            elog(ERROR, "TID lookup array doesn't match verification array");
    }

    PG_RETURN_VOID();
}
```
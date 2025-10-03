# brinSetHeapBlockItemptr

## Location
[src/backend/access/brin/brin_revmap.c:155-193](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_revmap.c#L155-L193)

## Overview
Sets the item pointer (TID) for a specific heap block in a BRIN revmap page, establishing the mapping from heap block range to index tuple location.

## Definition
```c
void brinSetHeapBlockItemptr(Buffer buf, BlockNumber pagesPerRange, BlockNumber heapBlk, ItemPointerData tid)
```

## Detailed Description
This function updates the revmap entry for a specific heap block by setting its corresponding item pointer to point to the BRIN index tuple that summarizes that block range. The revmap serves as a directory that maps heap block numbers to their corresponding index tuples, enabling efficient lookup of summary information during queries.

The function performs the following operations:
1. Extracts the page contents from the provided buffer
2. Casts the page contents to RevmapContents structure to access the TID array
3. Calculates the array index for the given heap block using HEAPBLK_TO_REVMAP_INDEX
4. Sets the item pointer at that index to the provided TID value
5. Handles both valid TIDs (sets the pointer) and invalid TIDs (marks as invalid)

The function is used in both normal operations (during index maintenance) and WAL replay scenarios. The caller is responsible for ensuring the buffer is properly locked and for updating the LSN after the operation completes.

## Parameters / Member Variables
- `buf`: Buffer containing the revmap page to update (must be locked by caller)
- `pagesPerRange`: Number of heap pages covered by each BRIN range (used for index calculation)
- `heapBlk`: The heap block number whose revmap entry should be updated
- `tid`: The item pointer (TID) to store, pointing to the corresponding BRIN index tuple

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageGetContents](../P/PageGetContents.md)
  - HEAPBLK_TO_REVMAP_INDEX
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
  - [ItemPointerSet](../I/ItemPointerSet.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
- Types referenced:
  - [RevmapContents](../R/RevmapContents.md)
  - [ItemPointerData](../I/ItemPointerData.md)
  - Page
  - Buffer
  - BlockNumber
- Called from:
  - [brin_doupdate](brin_doupdate.md)
  - [brin_doinsert](brin_doinsert.md)
  - [brinRevmapDesummarizeRange](brinRevmapDesummarizeRange.md)
  - [brin_xlog_insert_update](brin_xlog_insert_update.md)
  - [brin_xlog_desummarize_page](brin_xlog_desummarize_page.md)

## Notes and Other Information
- The caller must ensure the buffer is properly locked before calling this function
- The caller is responsible for updating the page LSN after this operation
- This function works with both valid and invalid TIDs - invalid TIDs mark ranges as not having summary tuples
- Used in both regular operations and WAL replay, making it critical for crash recovery
- The RevmapContents structure contains a flexible array of ItemPointerData that fills the available page space
- The HEAPBLK_TO_REVMAP_INDEX macro efficiently calculates the array position for a given heap block within its range

## Simplified Source

```c
void brinSetHeapBlockItemptr(Buffer buf, BlockNumber pagesPerRange,
                            BlockNumber heapBlk, ItemPointerData tid)
{
    // Get the revmap page contents from buffer
    Page page = BufferGetPage(buf);
    RevmapContents *contents = (RevmapContents *) PageGetContents(page);

    // Calculate position in TID array for this heap block
    ItemPointerData *target_ptr = contents->rm_tids +
                                  HEAPBLK_TO_REVMAP_INDEX(pagesPerRange, heapBlk);

    // Set or clear the item pointer
    if (ItemPointerIsValid(&tid)) {
        ItemPointerSet(target_ptr,
                      ItemPointerGetBlockNumber(&tid),
                      ItemPointerGetOffsetNumber(&tid));
    } else {
        ItemPointerSetInvalid(target_ptr);
    }
}
```
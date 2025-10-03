# tbm_shared_iterate

## Location
[src/backend/nodes/tidbitmap.c:1052-1145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L1052-L1145)

## Overview
Scans through the next page of a TIDBitmap using a shared iterator that can be safely accessed across multiple processes with proper locking.

## Definition

```c
TBMIterateResult *
tbm_shared_iterate(TBMSharedIterator *iterator)
```
## Detailed Description
The  function provides the same core functionality as  but with multi-process safety through LWLock synchronization. Before accessing shared iterator state, it acquires an exclusive LWLock to prevent race conditions between concurrent processes.

Like its private counterpart, it handles both lossy chunks and exact pages, ensuring numerical page order. The key difference is that all iteration state is stored in shared memory structures accessible to multiple processes, requiring careful lock management around state modifications.

The function uses shared page and chunk index arrays to access the actual PagetableEntry data, allowing multiple processes to coordinate iteration over the same bitmap data structure.

## Parameters / Member Variables
- `*iterator`: TBMSharedIterator pointer containing shared iteration state and references to shared memory segments
## Dependencies
- Functions called/Symbols referenced:
  - [tbm_advance_schunkbit](tbm_advance_schunkbit.md)
  - [tbm_extract_page_tuple](tbm_extract_page_tuple.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [TBMSharedIterator](../T/TBMSharedIterator.md)
  - [TBMSharedIteratorState](../T/TBMSharedIteratorState.md)
  - [TBMIterateResult](../T/TBMIterateResult.md)
  - [PagetableEntry](../P/PagetableEntry.md)
  - PAGES_PER_CHUNK
- Called from (representative examples):
  - [BitmapHeapNext](../B/BitmapHeapNext.md) (src/backend/executor/nodeBitmapHeapscan.c:241)
  - [BitmapAdjustPrefetchIterator](../B/BitmapAdjustPrefetchIterator.md) (src/backend/executor/nodeBitmapHeapscan.c:408)
  - [BitmapPrefetch](../B/BitmapPrefetch.md) (src/backend/executor/nodeBitmapHeapscan.c:534)

## Notes and Other Information
Critical for parallel bitmap heap scans where multiple worker processes need to coordinate access to the same bitmap. The LWLock ensures atomic updates to shared iteration state. All shared memory pointers must be properly initialized before calling this function. The function releases the lock before returning, whether successful or at end of iteration.

## Simplified Source

```c
TBMIterateResult *
tbm_shared_iterate(TBMSharedIterator *iterator)
{
    TBMIterateResult *output = &iterator->output;
    TBMSharedIteratorState *istate = iterator->state;
    PagetableEntry *ptbase = NULL;
    int *idxpages = NULL;
    int *idxchunks = NULL;

    // Get local pointers to shared arrays
    if (iterator->ptbase != NULL)
        ptbase = iterator->ptbase->ptentry;
    if (iterator->ptpages != NULL)
        idxpages = iterator->ptpages->index;
    if (iterator->ptchunks != NULL)
        idxchunks = iterator->ptchunks->index;

    // Acquire lock for shared state access
    LWLockAcquire(&istate->lock, LW_EXCLUSIVE);

    // Advance to next set bit in lossy chunks
    while (istate->schunkptr < istate->nchunks) {
        PagetableEntry *chunk = &ptbase[idxchunks[istate->schunkptr]];
        int schunkbit = istate->schunkbit;

        tbm_advance_schunkbit(chunk, &schunkbit);
        if (schunkbit < PAGES_PER_CHUNK) {
            istate->schunkbit = schunkbit;
            break;
        }
        // Move to next chunk
        istate->schunkptr++;
        istate->schunkbit = 0;
    }

    // Return chunk page if it comes before individual pages
    if (istate->schunkptr < istate->nchunks) {
        PagetableEntry *chunk = &ptbase[idxchunks[istate->schunkptr]];
        BlockNumber chunk_blockno = chunk->blockno + istate->schunkbit;

        if (istate->spageptr >= istate->npages ||
            chunk_blockno < ptbase[idxpages[istate->spageptr]].blockno) {
            // Return lossy page from chunk
            output->blockno = chunk_blockno;
            output->ntuples = -1;   // Lossy - check all tuples
            output->recheck = true;
            istate->schunkbit++;

            LWLockRelease(&istate->lock);
            return output;
        }
    }

    // Return individual page if available
    if (istate->spageptr < istate->npages) {
        PagetableEntry *page = &ptbase[idxpages[istate->spageptr]];
        int ntuples;

        // Extract exact tuple offsets
        ntuples = tbm_extract_page_tuple(page, output);
        output->blockno = page->blockno;
        output->ntuples = ntuples;
        output->recheck = page->recheck;
        istate->spageptr++;

        LWLockRelease(&istate->lock);
        return output;
    }

    LWLockRelease(&istate->lock);

    // No more pages
    return NULL;
}
```
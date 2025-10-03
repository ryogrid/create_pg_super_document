# tbm_begin_iterate

## Location
[src/backend/nodes/tidbitmap.c:689-765](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L689-L765)

## Overview
Creates and initializes a TBMIterator to prepare for sequential iteration through the tuple identifiers stored in a TIDBitmap.

## Definition
```c
TBMIterator *tbm_begin_iterate(TIDBitmap *tbm)
```

## Detailed Description
This function sets up the necessary data structures and state for iterating through a TIDBitmap. It creates a TBMIterator in the callers memory context with sufficient space for storing tuple offsets. The function handles both hash table and single-page bitmap modes, converting hash table entries into sorted arrays for efficient sequential access. When the bitmap is in TBM_HASH mode and not already prepared for iteration, it creates sorted arrays of pages and chunks from the hash table. The sorting ensures that tuples are returned in a predictable order during iteration. Once this function is called, the bitmap content becomes read-only to maintain iteration consistency.

## Parameters / Member Variables
- `tbm`: Pointer to the TIDBitmap structure to be prepared for iteration

## Dependencies
- Functions called/Symbols referenced:
  - [TBMIterator](../T/TBMIterator.md) (structure type)
  - TBM_ITERATING_SHARED (constant)
  - MAX_TUPLES_PER_PAGE (constant)
  - TBM_HASH (constant)
  - TBM_NOT_ITERATING (constant)
  - [PagetableEntry](../P/PagetableEntry.md) (structure type)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (function)
  - qsort (function)
  - [tbm_comparator](tbm_comparator.md) (function)
  - TBM_ITERATING_PRIVATE (constant)
- Called from (representative examples):
  - [startScanEntry](../s/startScanEntry.md) (src/backend/access/gin/ginget.c:388)
  - [BitmapHeapNext](../B/BitmapHeapNext.md) (src/backend/executor/nodeBitmapHeapscan.c:116, 122)

## Notes and Other Information
- The iterator is created in the callers memory context and includes trailing space for tuple offsets
- After calling this function, the bitmap contents must not be modified
- Multiple iterators can be created for the same bitmap to enable parallel scanning
- The function converts hash table entries to sorted arrays for efficient sequential access
- The TBMIterator includes space for MAX_TUPLES_PER_PAGE OffsetNumbers in its trailing space
- Proper cleanup should be done with tbm_end_iterate, though memory context release is also acceptable

## Simplified Source

```c
TBMIterator *
tbm_begin_iterate(TIDBitmap *tbm)
{
    TBMIterator *iterator;

    Assert(tbm->iterating != TBM_ITERATING_SHARED);

    // Create iterator with space for tuple offsets
    iterator = (TBMIterator *) palloc(sizeof(TBMIterator) +
                                     MAX_TUPLES_PER_PAGE * sizeof(OffsetNumber));
    iterator->tbm = tbm;

    // Initialize iteration pointers
    iterator->spageptr = 0;
    iterator->schunkptr = 0;
    iterator->schunkbit = 0;

    // Convert hash table to sorted arrays if needed
    if (tbm->status == TBM_HASH && tbm->iterating == TBM_NOT_ITERATING) {
        pagetable_iterator i;
        PagetableEntry *page;
        int npages = 0, nchunks = 0;

        // Allocate page and chunk arrays
        if (!tbm->spages && tbm->npages > 0)
            tbm->spages = (PagetableEntry **) MemoryContextAlloc(tbm->mcxt,
                                                                tbm->npages * sizeof(PagetableEntry *));
        if (!tbm->schunks && tbm->nchunks > 0)
            tbm->schunks = (PagetableEntry **) MemoryContextAlloc(tbm->mcxt,
                                                                 tbm->nchunks * sizeof(PagetableEntry *));

        // Extract entries from hash table
        pagetable_start_iterate(tbm->pagetable, &i);
        while ((page = pagetable_iterate(tbm->pagetable, &i)) != NULL) {
            if (page->ischunk)
                tbm->schunks[nchunks++] = page;
            else
                tbm->spages[npages++] = page;
        }

        // Sort arrays for ordered iteration
        if (npages > 1)
            qsort(tbm->spages, npages, sizeof(PagetableEntry *), tbm_comparator);
        if (nchunks > 1)
            qsort(tbm->schunks, nchunks, sizeof(PagetableEntry *), tbm_comparator);
    }

    tbm->iterating = TBM_ITERATING_PRIVATE;
    return iterator;
}
```
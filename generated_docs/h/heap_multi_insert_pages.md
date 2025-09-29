# heap_multi_insert_pages

## Location
[src/backend/access/heap/heapam.c:2277-2308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L2277-L2308)

## Overview
heap_multi_insert_pages is a static helper function for heap_multi_insert() that computes the number of entire pages required to insert the remaining heap tuples, used to determine how much a relation needs to be extended.

## Definition

```c
static int
heap_multi_insert_pages(HeapTuple *heaptuples, int done, int ntuples, Size saveFreeSpace)
```
## Detailed Description
This function calculates how many pages will be needed to accommodate the remaining tuples in a multi-insert operation. It simulates the insertion process by iterating through the tuples starting from the 'done' index and tracking available space on each page. When a tuple cannot fit on the current page, it increments the page count and starts a new page. The calculation includes space for both the tuple data and its item identifier, ensuring accurate page count estimation for relation extension.

## Parameters / Member Variables
- `heaptuples`: Array of HeapTuple pointers containing the tuples to be inserted
- `done`: Index of the first tuple that hasn't been processed yet
- `ntuples`: Total number of tuples in the heaptuples array
- `saveFreeSpace`: Amount of free space to preserve on each page

## Dependencies
- Functions called/Symbols referenced:
  - SizeOfPageHeaderData (constant for page header size)
  - [ItemIdData](../I/ItemIdData.md) (struct for item identifier size calculation)
  - MAXALIGN (macro for alignment calculation)
- Called from:
  - [heap_multi_insert](heap_multi_insert.md)

## Notes and Other Information
- This is a static function, only accessible within heapam.c
- The function accounts for both tuple data size and ItemIdData overhead
- Uses MAXALIGN to ensure proper tuple alignment on pages
- The calculation helps optimize relation extension by determining exact page requirements upfront
- Returns the number of pages needed starting from 1 (assumes at least one page is needed)

## Simplified Source

```c
static int
heap_multi_insert_pages(HeapTuple *heaptuples, int done, int ntuples, Size saveFreeSpace)
{
    // Calculate available space per page (total page size minus header and reserved space)
    size_t page_avail = BLCKSZ - SizeOfPageHeaderData - saveFreeSpace;
    int npages = 1;  // Start with at least one page

    // Iterate through remaining tuples to count required pages
    for (int i = done; i < ntuples; i++) {
        // Calculate space needed for this tuple (data + item identifier)
        size_t tup_sz = sizeof(ItemIdData) + MAXALIGN(heaptuples[i]->t_len);

        // Check if tuple fits on current page
        if (page_avail < tup_sz) {
            // Need a new page
            npages++;
            page_avail = BLCKSZ - SizeOfPageHeaderData - saveFreeSpace;
        }

        // Consume space for this tuple
        page_avail -= tup_sz;
    }

    return npages;
}
```
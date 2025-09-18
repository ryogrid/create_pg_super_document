# TBMSharedIterator

## Location
src/backend/nodes/tidbitmap.c: 219 - 242

## Overview
TBMSharedIterator is a structure designed for joint iteration over TID (tuple identifier) bitmaps in shared memory contexts, extending the functionality of TBMIterator to support concurrent access across multiple processes in parallel bitmap heap scans.

## Definition


## Detailed Description
TBMSharedIterator serves as the shared memory counterpart to TBMIterator, specifically designed for joint iteration scenarios where multiple processes need to coordinate access to the same TID bitmap. This structure maintains references to shared state and provides organized access to both exact pages and lossy pages through separate PTIterationArray structures.

The iterator distinguishes between exact pages (where individual TIDs are stored) and lossy pages (where entire pages are marked as containing relevant tuples). This separation allows for efficient processing of different types of bitmap entries. The structure is used primarily in parallel bitmap heap scans where worker processes need to coordinate their iteration over the same set of pages.

The output member is marked as variable-size and must be last in the structure, indicating that it can accommodate different result sizes depending on the specific iteration context.

## Parameters / Member Variables
- : Pointer to TBMSharedIteratorState that contains the shared state information used for coordinating between multiple iterators in parallel execution contexts.
- : Pointer to PTEntryArray containing the base pagetable element array with all the page entries that can be iterated over.
- : Pointer to PTIterationArray containing a sorted list of indices for exact pages, where individual TID bits are tracked precisely.
- : Pointer to PTIterationArray containing a sorted list of indices for lossy pages, where entire pages are marked as containing relevant tuples without individual TID tracking.
- : TBMIterateResult structure that holds the current iteration result. This must be the last member due to its variable-size nature.

## Dependencies
- Functions called/Symbols referenced:
  - TBMSharedIteratorState (shared state management)
  - PTEntryArray (pagetable entry storage)
  - PTIterationArray (iteration index management)
  - TBMIterateResult (iteration output results)
- Called from (representative examples):
  - BitmapHeapNext (in nodeBitmapHeapscan.c)
  - BitmapAdjustPrefetchIterator
  - BitmapPrefetch
  - tbm_shared_iterate
  - tbm_end_shared_iterate
  - tbm_attach_shared_iterate

## Notes and Other Information
- This structure is part of PostgreSQL's parallel bitmap heap scan implementation
- Unlike TBMIterator, this version is specifically designed for shared memory usage and joint iteration across multiple processes
- The separation of ptpages (exact) and ptchunks (lossy) allows for optimized processing of different bitmap storage strategies
- The variable-size output member requires careful memory management and must remain the last field in the structure
- Used primarily in executor nodes for bitmap heap scans where parallel processing is enabled
- The structure supports both prefetch operations and actual tuple retrieval in bitmap heap scan operations
- Memory layout is critical due to shared memory usage patterns in parallel query execution
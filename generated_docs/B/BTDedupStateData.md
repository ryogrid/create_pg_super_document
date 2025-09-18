# BTDedupStateData

## Location
src/include/access/nbtree.h: 865 - 891

## Overview
BTDedupStateData is a comprehensive working area structure used during B-tree deduplication operations to track the state of a whole-page deduplication pass and manage pending posting lists.

## Definition


## Detailed Description
BTDedupStateData serves as a comprehensive state management structure for B-tree deduplication operations. It tracks both the overall progress of a deduplication pass across an entire page and the specific details of the current pending posting list being constructed.

The structure manages the complex process of identifying groups of duplicate tuples and combining them into posting list tuples to save space. It tracks physical size calculations to determine space savings, manages the heap TID arrays that form the core of posting lists, and maintains an array of intervals representing groups of consecutive items to be processed.

The deduplication process involves examining tuples on a page, identifying ranges of duplicates, and creating new posting list tuples that combine multiple identical key values with different heap TIDs into a single, more compact tuple.

## Parameters / Member Variables
- : Boolean flag indicating whether the page is still being deduplicated
- : Counter for the number of max-sized tuples encountered so far
- : Size limit for the final posting list tuple
- : IndexTuple used as the base to form the new posting list
- : Page offset number of the base tuple
- : Size of the base tuple without its original posting list
- : Array of heap TIDs that will comprise the pending posting list
- : Number of heap TIDs currently in the htids array
- : Number of existing tuples/line pointers being consolidated
- : Physical tuple size including line pointer overhead
- : Current number of intervals in the intervals array
- : Array of BTDedupInterval structures representing groups of consecutive items

## Dependencies
- Functions called/Symbols referenced:
  - MaxIndexTuplesPerPage (constant)
  - [BTDedupInterval](BTDedupInterval.md) (type)
  - [IndexTuple](../I/IndexTuple.md) (type)
  - OffsetNumber (type)
  - Size (type)
  - ItemPointer (type)
- Called from (representative examples):
  - [_bt_dedup_pass](../b/_bt_dedup_pass.md)
  - [_bt_bottomupdel_pass](../b/_bt_bottomupdel_pass.md)
  - _bt_load
  - [btree_xlog_dedup](../b/btree_xlog_dedup.md)
  - BTDedupState (typedef alias)

## Notes and Other Information
- Central to PostgreSQL's B-tree space optimization through tuple deduplication
- Manages complex state transitions during the deduplication process
- The intervals array tracks groups of consecutive items for efficient batch processing
- Physical size tracking enables accurate calculation of space savings
- Used in both regular deduplication operations and during index builds
- Essential for WAL logging and recovery of deduplication operations
- The structure supports both creating new posting lists and extending existing ones
- Designed to handle the complexity of mixed regular and posting list tuple scenarios
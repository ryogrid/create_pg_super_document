# TidStoreIterateNext

## Location
[src/backend/access/common/tidstore.c:511-535](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tidstore.c#L511-L535)

## Overview
Scans the TidStore and returns the TIDs of the next block during iteration, with offsets and block numbers returned in ordered sequence.

## Definition


## Detailed Description
This function advances the TidStore iteration to the next block and returns the tuple identifiers for that block. It retrieves the next key-value pair from the underlying tree structure and extracts all TIDs associated with the block number encoded in the key. The function ensures that offsets within each iteration result are ordered, and block numbers are returned in order across all iterations.

The function delegates to specialized iteration functions based on whether the TidStore is shared or local, then processes the retrieved page data to extract TIDs into the iterator's output structure.

## Parameters / Member Variables
- : The TidStoreIter structure containing iteration state

## Dependencies
- Functions called/Symbols referenced:
  - TidStoreIsShared
  - shared_ts_iterate_next
  - local_ts_iterate_next
  - [tidstore_iter_extract_tids](../t/tidstore_iter_extract_tids.md)
  - [BlocktableEntry](../B/BlocktableEntry.md)
- Called from (representative examples):
  - [lazy_vacuum_heap_rel](../l/lazy_vacuum_heap_rel.md)
  - [check_set_block_offsets](../c/check_set_block_offsets.md)

## Notes and Other Information
- Returns NULL when the iteration is complete (no more blocks to process)
- The returned TidStoreIterResult pointer references the iterator's internal output buffer
- Block numbers are returned in ascending order across iterations
- Offsets within each block are guaranteed to be ordered
- The function automatically handles both shared and local TidStore variants
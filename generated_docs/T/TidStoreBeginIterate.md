# TidStoreBeginIterate

## Location
[src/backend/access/common/tidstore.c:482-510](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tidstore.c#L482-L510)

## Overview
Prepares to iterate through a TidStore by creating and initializing a TidStoreIter structure that manages the iteration state.

## Definition


## Detailed Description
This function initializes the iteration process for a TidStore, creating a TidStoreIter structure that maintains the state needed to traverse all tuple identifiers stored in the TidStore. The function allocates memory for the iterator structure and its associated output buffer, and sets up the appropriate iterator based on whether the TidStore is shared (multi-process) or local (single-process).

The iterator is designed to efficiently process bitmap data by pre-allocating an output array sized to handle at least one completely full bitmap element. The function delegates to specialized iterator initialization functions based on the TidStore type.

## Parameters / Member Variables
- : The TidStore to iterate over

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [palloc](../p/palloc.md)
  - TidStoreIsShared
  - shared_ts_begin_iterate
  - local_ts_begin_iterate
  - BITS_PER_BITMAPWORD
- Called from (representative examples):
  - [lazy_vacuum_heap_rel](../l/lazy_vacuum_heap_rel.md)
  - [check_set_block_offsets](../c/check_set_block_offsets.md)

## Notes and Other Information
- The TidStoreIter struct is created in the caller's memory context and must be freed using TidStoreEndIterate
- The caller is responsible for maintaining appropriate locking on the TidStore throughout the iteration process
- The output buffer is initially sized to contain offsets from one completely full bitmap element (2 * BITS_PER_BITMAPWORD)
- The function handles both shared and local TidStore variants automatically
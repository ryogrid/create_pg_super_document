# validate_index_callback

## Location
src/backend/catalog/index.c: 3422 - 3441

## Overview
A bulkdelete callback function that collects index TIDs during the validation phase of concurrent index building, storing them in a tuplesort for later merging.

## Definition
```c
static bool validate_index_callback(ItemPointer itemptr, void *opaque)
```

## Detailed Description
validate_index_callback serves as a callback function for index_bulk_delete during the index validation phase of concurrent index building. Instead of actually deleting index entries, this callback collects all TIDs (tuple identifiers) found in the index and stores them in a tuplesort object. The function encodes ItemPointers as int64 values for efficient sorting and processing.

The callback is designed to gather a complete inventory of all tuples currently present in the index, which will later be used in a merge-join operation against a table scan to identify any missing tuples that need to be inserted into the index.

## Parameters / Member Variables
- : Pointer to the current index tuple's TID being processed during the bulk delete scan
- : Void pointer to ValidateIndexState structure containing the tuplesort object and counters

## Dependencies
- Functions called/Symbols referenced:
  - itemptr_encode
  - Int64GetDatum
  - tuplesort_putdatum
  - ValidateIndexState
- Called from (representative examples):
  - validate_index

## Notes and Other Information
- This function is static and only used internally during concurrent index validation
- Always returns false to ensure no actual deletion occurs - it's purely a collection operation
- Encodes TIDs as int64 values for performance reasons during sorting
- Maintains a counter (itups) to track the number of index tuples processed
- Part of PostgreSQL's sophisticated concurrent index building mechanism that minimizes locking overhead
- The collected TIDs will be sorted and used for efficient merge-join processing during table validation
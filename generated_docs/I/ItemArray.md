# ItemArray

## Location
src/test/modules/test_tidstore/test_tidstore.c: 40 - 47

## Overview
ItemArray is a test data structure used for verification in the TidStore test module that stores multiple arrays of ItemPointerData for different testing purposes.

## Definition
```c
typedef struct ItemArray
{
    ItemPointerData *insert_tids;
    ItemPointerData *lookup_tids;
    ItemPointerData *iter_tids;
    int             max_tids;
    int             num_tids;
} ItemArray;
```

## Detailed Description
ItemArray is a helper structure defined in the test_tidstore module that manages arrays of tuple identifiers (TIDs) for comprehensive testing of the TidStore functionality. This structure maintains separate arrays for tracking TIDs that were inserted, looked up, and iterated over, allowing the test suite to verify that TidStore operations work correctly by comparing results across different access patterns.

The structure supports dynamic resizing of its arrays and is used throughout the test functions to maintain a parallel verification dataset alongside the actual TidStore being tested. It serves as a reference implementation to validate that TidStore operations (insert, lookup, iterate) produce consistent and expected results.

## Parameters / Member Variables
- `insert_tids`: Array of ItemPointerData storing TIDs that have been inserted into the TidStore for verification purposes
- `lookup_tids`: Array of ItemPointerData storing TIDs found during lookup operations, used to verify successful retrievals
- `iter_tids`: Array of ItemPointerData storing TIDs collected during iteration over the TidStore
- `max_tids`: Maximum capacity of the TID arrays, used for dynamic resizing when more space is needed
- `num_tids`: Current number of valid TIDs stored in the arrays

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerData](ItemPointerData.md) (PostgreSQL tuple identifier structure)
- Called from (representative examples):
  - Static variable `items` in test_tidstore.c
  - Used by test functions: test_create, do_set_block_offsets, check_set_block_offsets, test_destroy

## Notes and Other Information
- This is a test-only structure located in src/test/modules/test_tidstore/test_tidstore.c
- The arrays are dynamically resized (doubled) when capacity is exceeded
- All three TID arrays (insert_tids, lookup_tids, iter_tids) are maintained in parallel for cross-verification
- Used exclusively for testing TidStore functionality and ensuring correctness of operations
- Memory is managed using PostgreSQL's memory allocation functions (palloc, repalloc, pfree)
# TransactionIdInArray

## Location
src/backend/replication/logical/reorderbuffer.c: 5303 - 5312

## Overview
Efficiently searches for a transaction ID within a pre-sorted array using binary search to determine transaction visibility for PostgreSQL's MVCC system.

## Definition
```c
static bool TransactionIdInArray(TransactionId xid, TransactionId *xip, Size num)
```

## Detailed Description
This function provides an efficient O(log n) lookup mechanism to determine if a specific transaction ID exists within a sorted array of transaction IDs. It is a critical component of PostgreSQL's Multi-Version Concurrency Control (MVCC) system, particularly used in visibility checks where the system needs to determine whether a transaction is part of a specific set of active or committed transactions.

The function uses the standard C library's bsearch() function to perform binary search on the pre-sorted array. This is much more efficient than linear search when dealing with large arrays of transaction IDs, which is common in PostgreSQL's transaction visibility determinations.

The function includes a safety check to ensure the array is not empty before attempting the search, preventing unnecessary search operations on empty arrays.

## Parameters / Member Variables
- `xid`: The transaction ID to search for in the array
- `xip`: Pointer to the pre-sorted array of transaction IDs
- `num`: The number of elements in the transaction ID array

## Dependencies
- Functions called/Symbols referenced:
  - bsearch (standard C library function)
  - xidComparator (comparison function for transaction IDs)
- Called from (representative examples):
  - HeapTupleSatisfiesHistoricMVCC (multiple times for different visibility checks)
  - UpdateLogicalMappings

## Notes and Other Information
- This is a static function, accessible only within heapam_visibility.c
- Requires the input array to be pre-sorted for binary search to work correctly
- Returns true if the transaction ID is found, false otherwise
- The function is highly optimized for performance since it's called frequently during tuple visibility checks
- Essential for implementing PostgreSQL's snapshot isolation and determining which tuples should be visible to specific transactions
- The xidComparator function is used to properly compare transaction IDs, handling wraparound and other PostgreSQL-specific transaction ID semantics
- Used extensively in historic MVCC visibility checks where the system needs to determine if a transaction was active at a specific point in time
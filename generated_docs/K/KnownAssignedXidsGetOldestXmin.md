# KnownAssignedXidsGetOldestXmin

## Location
[src/backend/storage/ipc/procarray.c:5182-5216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L5182-L5216)

## Overview
KnownAssignedXidsGetOldestXmin retrieves the oldest (minimum) transaction ID from the KnownAssignedXids array, returning InvalidTransactionId if the array is empty.

## Definition

```c
static TransactionId
KnownAssignedXidsGetOldestXmin(void)
```
## Detailed Description
This function efficiently finds the oldest transaction ID in the KnownAssignedXids array by taking advantage of the array's sorted nature. Since the KnownAssignedXids array maintains transaction IDs in sorted order, the function only needs to find the first valid entry to determine the minimum value. The function:

1. Captures the current head and tail positions of the KnownAssignedXids array
2. Uses a read barrier to ensure memory consistency with concurrent operations
3. Iterates from the tail toward the head, looking for the first valid entry
4. Returns the first valid transaction ID found (which is the oldest due to sorting)
5. Returns InvalidTransactionId if no valid entries are found

This function is essential for determining transaction visibility horizons and managing cleanup operations in PostgreSQL's Hot Standby implementation.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - pg_read_barrier
  - InvalidTransactionId (constant)
- Called from (representative examples):
  - xc_slow_answer_inc
  - ComputeXidHorizons

## Notes and Other Information
- This is a static function accessible only within procarray.c
- Leverages the sorted property of KnownAssignedXids array for O(n) performance in worst case, but typically O(1) if the first entry is valid
- Uses memory barriers for safe concurrent access to shared data structures
- Part of PostgreSQL's Hot Standby recovery mechanism for transaction management
- The function is used to determine the oldest active transaction for cleanup and visibility decisions
- No locking requirements are explicitly mentioned in the code, but typically called while holding appropriate locks on the procArray structure
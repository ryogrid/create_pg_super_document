# cleanup_subxact_info

## Location
src/backend/replication/logical/worker.c: 4401 - 4417

## Overview
A static inline function that cleans up memory allocated for subtransaction information and resets related variables in logical replication workers.

## Definition
```c
static inline void cleanup_subxact_info()
```

## Detailed Description
cleanup_subxact_info is a memory management function that performs cleanup operations for subtransaction data structures used in logical replication. The function is responsible for deallocating dynamically allocated memory for subtransaction arrays and resetting all related tracking variables to their initial state.

The function operates on the global subxact_data structure, which maintains information about subtransactions during logical replication processing. When called, it:
1. Frees the dynamically allocated subtransaction array if it exists
2. Resets the pointer to NULL
3. Resets the last subtransaction ID to invalid
4. Zeros out the current and maximum subtransaction counts

This cleanup is essential for preventing memory leaks and ensuring proper state reset between transaction processing cycles.

## Parameters / Member Variables
This function takes no parameters and operates on the global subxact_data structure.

## Dependencies
- Functions called/Symbols referenced:
  - pfree (PostgreSQL memory deallocation function)
  - InvalidTransactionId (constant for invalid transaction ID)
  - subxact_data (global structure containing subtransaction information)
- Called from (representative examples):
  - stream_abort_internal (at src/backend/replication/logical/worker.c:1783)
  - subxact_info_write (at src/backend/replication/logical/worker.c:4033)
  - subxact_info_write (at src/backend/replication/logical/worker.c:4057)

## Notes and Other Information
- Marked as static inline for performance optimization since it's a simple cleanup function
- Essential for memory management in logical replication subtransaction handling
- Resets all subtransaction tracking variables to ensure clean state
- Called during transaction abort and after writing subtransaction information to ensure proper cleanup
- Part of the subtransaction tracking subsystem in PostgreSQL's logical replication worker
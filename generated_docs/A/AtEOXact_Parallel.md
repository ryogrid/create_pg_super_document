# AtEOXact_Parallel

## Location
[src/backend/access/transam/parallel.c:1271-1287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/parallel.c#L1271-L1287)

## Overview
Performs end-of-transaction cleanup for parallel contexts by destroying all remaining parallel contexts, ensuring complete cleanup at transaction boundaries.

## Definition

```c
enumsspace;
```
## Detailed Description
This function is called during transaction cleanup (both commit and abort) to ensure that all parallel contexts are properly destroyed at the end of a transaction. Unlike AtEOSubXact_Parallel which only cleans up contexts from a specific subtransaction, this function unconditionally destroys ALL remaining parallel contexts regardless of which subtransaction created them.

The function iterates through the global list of parallel contexts (pcxt_list) and destroys each one using DestroyParallelContext(). It continues until the list is completely empty, ensuring no parallel contexts survive the transaction boundary.

If this is a transaction commit and any parallel contexts still exist, the function logs a WARNING message indicating a resource leak. In properly written code, all parallel contexts should be explicitly destroyed before transaction commit.

This function serves as a safety net to prevent parallel context leaks that could accumulate over time and cause resource exhaustion. It ensures that transaction boundaries provide a clean slate for parallel context management.

## Parameters / Member Variables
- : Boolean flag indicating whether this is a transaction commit (true) or abort (false). Used to determine whether to log warnings for leaked contexts.

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - dlist_head_element
  - DestroyParallelContext
  - elog

- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md)
  - [AbortTransaction](AbortTransaction.md)
  - IsParallelWorker (referenced in header)

## Notes and Other Information
- This function provides the final cleanup for all parallel contexts at transaction end
- Unlike subtransaction cleanup, this processes ALL contexts regardless of their origin subtransaction
- WARNING messages help identify programming errors where contexts aren't properly cleaned up
- The function serves as a critical safety mechanism to prevent resource leaks
- It's called for both successful commits and transaction aborts
- Parallel contexts that survive to transaction end typically indicate application logic errors
- The function ensures system stability by preventing unbounded accumulation of parallel resources
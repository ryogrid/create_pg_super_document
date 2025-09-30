# sts_end_parallel_scan

## Location
[src/backend/utils/sort/sharedtuplestore.c:281-299](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/sharedtuplestore.c#L281-L299)

## Overview
Finishes a parallel scan of a shared tuplestore and frees associated backend-local resources.

## Definition
void sts_end_parallel_scan(SharedTuplestoreAccessor *accessor)

## Detailed Description
This function terminates a parallel scan that is currently in progress and cleans up backend-local resources. The primary operation is closing the currently open read file handle if one exists, and setting the read_file pointer to NULL to indicate that no scan is active.

The function includes a comment noting that in SHARED_TUPLESTORE_SINGLE_PASS mode, it could potentially delete all files at this point, but this would require implementing a reference count mechanism to track active parallel scanners to ensure safe deletion only when the reference count reaches zero.

## Parameters / Member Variables
- `accessor`: A pointer to the SharedTuplestoreAccessor structure whose scan should be terminated

## Dependencies
- Functions called/Symbols referenced:
  - [SharedTuplestoreAccessor](../S/SharedTuplestoreAccessor.md) (structure type)
  - [BufFileClose](../B/BufFileClose.md) (function to close buffer files)
- Called from (representative examples):
  - [sts_begin_parallel_scan](sts_begin_parallel_scan.md) (automatically called to end existing scans)
  - [ExecParallelHashRepartitionRest](../E/ExecParallelHashRepartitionRest.md) (in nodeHash.c:1429)
  - [ExecParallelPrepHashTableForUnmatched](../E/ExecParallelPrepHashTableForUnmatched.md) (in nodeHash.c:2126, 2127)
  - [ExecParallelHashCloseBatchAccessors](../E/ExecParallelHashCloseBatchAccessors.md) (in nodeHash.c:3193, 3194)
  - [ExecHashTableDetachBatch](../E/ExecHashTableDetachBatch.md) (in nodeHash.c:3299, 3300)
  - [ExecHashTableDetach](../E/ExecHashTableDetach.md) (in nodeHash.c:3403, 3404)
  - [ExecParallelHashJoinNewBatch](../E/ExecParallelHashJoinNewBatch.md) (in nodeHashjoin.c:1240)

## Notes and Other Information
- Safe to call even when no scan is active (checks for NULL read_file)
- Automatically called by sts_begin_parallel_scan() to ensure clean state
- Does not affect the shared tuplestore data itself, only cleans up local read state
- In single-pass mode, there is potential for file cleanup optimization that is not currently implemented
- Part of PostgreSQLs parallel hash join infrastructure cleanup routines
- Essential for proper resource management in parallel query execution

## Simplified Source
```c
void sts_end_parallel_scan(SharedTuplestoreAccessor *accessor) {
    // Close read file handle if one exists
    if (accessor->read_file != NULL) {
        BufFileClose(accessor->read_file);
        accessor->read_file = NULL;
    }

    // Note: In SHARED_TUPLESTORE_SINGLE_PASS mode, we could delete all files
    // here, but would need reference counting to know when safe to do so
}
```
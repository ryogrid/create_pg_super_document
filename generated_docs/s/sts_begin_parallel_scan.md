# sts_begin_parallel_scan

## Location
[src/backend/utils/sort/sharedtuplestore.c:253-280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/sharedtuplestore.c#L253-L280)

## Overview
Initiates a parallel scan of a shared tuplestore, setting up the accessor to begin reading tuples from the store.

## Definition
void sts_begin_parallel_scan(SharedTuplestoreAccessor *accessor)

## Detailed Description
This function begins a parallel scan of the shared tuplestore contents. It first ends any existing scan that was in progress by calling sts_end_parallel_scan(), ensuring a clean state. The function then verifies that all participants have finished writing to their respective files (all buffers are flushed and files have stopped growing).

The function optimizes reading by starting with the file that the current backend wrote to, potentially taking advantage of caching locality. It initializes the read state by setting the read_participant to the current participant, clearing the read_file handle, and setting read_next_page to 0 to start reading from the beginning.

## Parameters / Member Variables
- `accessor`: A pointer to the SharedTuplestoreAccessor structure that provides access to the shared tuplestore to be scanned

## Dependencies
- Functions called/Symbols referenced:
  - [SharedTuplestoreAccessor](../S/SharedTuplestoreAccessor.md) (structure type)
  - [sts_end_parallel_scan](sts_end_parallel_scan.md) (function to end any existing scan)
  - PG_USED_FOR_ASSERTS_ONLY (macro for assertion-only variables)
- Called from (representative examples):
  - [ExecParallelHashRepartitionRest](../E/ExecParallelHashRepartitionRest.md) (in nodeHash.c:1408)
  - [ExecParallelHashJoinNewBatch](../E/ExecParallelHashJoinNewBatch.md) (in nodeHashjoin.c:1229, 1258)

## Notes and Other Information
- Automatically ends any existing scan in progress before beginning the new scan
- Asserts that all participants have finished writing before allowing the scan to begin
- Optimizes read performance by starting with the file written by the current backend
- Initializes read state to begin from page 0 of the selected participant file
- Part of PostgreSQLs parallel query execution infrastructure for hash joins and repartitioning
- Must be called after sts_reinitialize() and before calling sts_parallel_scan_next() in a typical scan cycle
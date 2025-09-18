# _brin_parallel_scan_and_build

## Location
[src/backend/access/brin/brin.c:2796-2852](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L2796-L2852)

## Overview
This function performs the core work of each parallel worker during BRIN index construction, handling table scanning, tuple processing, sorting, and coordination with other workers.

## Definition
```c
static void _brin_parallel_scan_and_build(BrinBuildState *state, BrinShared *brinshared, Sharedsort *sharedsort, Relation heap, Relation index, int sortmem, bool progress)
```

## Detailed Description
This function implements the main work loop for parallel BRIN index building. Each worker (including the leader when participating) executes this function to scan its assigned portion of the table, build BRIN ranges, and sort the results. The function coordinates with other workers through shared memory structures and uses tuplesort for efficient sorting of BRIN ranges. After completing its work, each worker updates shared statistics and signals completion to the leader process.

## Parameters / Member Variables
- `state`: The BRIN build state containing worker-specific information
- `brinshared`: Shared memory structure for coordinating between all workers
- `sharedsort`: Shared sorting coordination structure
- `heap`: The relation (table) being scanned and indexed
- `index`: The BRIN index relation being built
- `sortmem`: Amount of working memory allocated to this worker (in KB)
- `progress`: Boolean indicating whether to report progress (typically true for leader)

## Dependencies
- Functions called/Symbols referenced:
  - [tuplesort_begin_index_brin](../t/tuplesort_begin_index_brin.md): Initializes BRIN-specific tuplesort
  - [BuildIndexInfo](../B/BuildIndexInfo.md): Creates index information structure
  - [table_beginscan_parallel](../t/table_beginscan_parallel.md): Starts parallel table scan
  - ParallelTableScanFromBrinShared: Extracts parallel scan state from shared memory
  - [table_index_build_scan](../t/table_index_build_scan.md): Performs the actual table scan and tuple processing
  - [brinbuildCallbackParallel](brinbuildCallbackParallel.md): Callback function for processing each tuple
  - [form_and_spill_tuple](../f/form_and_spill_tuple.md): Finalizes the last BRIN range
  - tuplesort_performsort: Sorts the collected BRIN ranges
  - ConditionVariableSignal: Signals completion to leader
  - tuplesort_end: Cleans up sorting resources

- Called from (representative examples):
  - [_brin_leader_participate_as_worker](_brin_leader_participate_as_worker.md): When leader participates as worker
  - [_brin_parallel_build_main](_brin_parallel_build_main.md): The main entry point for parallel workers

## Notes and Other Information
- This is a static function, only accessible within the brin.c file
- Workers coordinate through spinlocks when updating shared statistics (reltuples, indtuples, nparticipantsdone)
- The function uses PostgreSQL's parallel table scan infrastructure to distribute work among workers
- Each worker maintains its own tuplesort state for sorting BRIN ranges
- Progress reporting is typically enabled only for the leader process
- The function handles both regular workers and the leader when it participates as a worker
- Memory allocation (sortmem) is pre-calculated and passed in to ensure fair resource distribution
- Completion is signaled through condition variables to allow efficient coordination
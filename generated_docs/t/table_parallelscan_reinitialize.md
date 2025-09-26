# table_parallelscan_reinitialize

## Location
[src/include/access/tableam.h:1175-1192](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1175-L1192)

## Overview
Restarts a parallel table scan by reinitializing the parallel scan descriptor, intended to be called by the leader process after all workers have finished.

## Definition
```c
static inline void
table_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
```

## Detailed Description
This function reinitializes a parallel table scan to restart from the beginning. It is designed to be called by the leader process in a parallel query execution scenario. The caller must ensure that all worker processes have completed their current scan operations before calling this function to avoid race conditions or inconsistent scan state.

The function delegates the actual reinitialization work to the table access method's parallelscan_reinitialize implementation, allowing different storage engines to handle parallel scan reinitialization according to their specific requirements.

## Parameters / Member Variables
- `rel`: The relation (table) being scanned
- `pscan`: The ParallelTableScanDesc to reinitialize

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelTableScanDesc](../P/ParallelTableScanDesc.md) (parameter type)
  - rel->rd_tableam->parallelscan_reinitialize (table access method function)
- Called from (representative examples):
  - [ExecSeqScanReInitializeDSM](../E/ExecSeqScanReInitializeDSM.md)

## Notes and Other Information
- This is an inline function defined in the table access method header
- Must be called by the leader process in parallel query execution
- Caller responsibility to ensure all workers have finished before reinitialization
- Used in parallel sequential scan operations for restarting scans
- The actual reinitialization logic is delegated to the specific table access method
- Critical for proper coordination in parallel query execution scenarios
- Prevents race conditions by requiring synchronization before reinitialization
# SetParallelStartTimestamps

## Location
src/backend/access/transam/xact.c: 856 - 866

## Overview
Sets the transaction and statement start timestamps for a parallel worker to inherit from its parent transaction.

## Definition


## Detailed Description
SetParallelStartTimestamps is specifically designed for parallel worker processes to inherit timestamp values from their parent transaction rather than generating their own. This ensures timestamp consistency between the leader process and parallel workers, which is crucial for maintaining proper transaction semantics and MVCC behavior across parallel operations. The function must be called by the parallel worker infrastructure before calling StartTransaction() or SetCurrentStatementStartTimestamp() to ensure proper timestamp initialization.

## Parameters / Member Variables
- : The transaction start timestamp to inherit from the parent transaction
- : The statement start timestamp to inherit from the parent transaction

## Dependencies
- Functions called/Symbols referenced:
  - IsParallelWorker (function to verify running in parallel worker context)
  - Assert (assertion macro for debugging)
  - xactStartTimestamp (global variable)
  - stmtStartTimestamp (global variable)
- Called from (representative examples):
  - ParallelWorkerMain (src/backend/access/transam/parallel.c:1398)

## Notes and Other Information
- This function should only be called in parallel worker processes (enforced by Assert)
- Essential for maintaining timestamp consistency across parallel query execution
- Must be called before transaction initialization functions in parallel workers
- Part of PostgreSQL's parallel query execution infrastructure
- Located in src/backend/access/transam/xact.c:856-866
- The timestamps ensure that all workers see a consistent view of time within the transaction
# ExecAggRetrieveInstrumentation

## Location
src/backend/executor/nodeAgg.c: 4742 - 4755

## Overview
This function transfers aggregate statistics from DSM (Dynamic Shared Memory) to private memory, ensuring instrumentation data persists after parallel execution completes.

## Definition
```c
void ExecAggRetrieveInstrumentation(AggState *node)
```

## Detailed Description
ExecAggRetrieveInstrumentation is responsible for copying aggregate instrumentation data from shared memory to private memory. This function is typically called at the end of parallel aggregate execution to preserve performance statistics and instrumentation data that was collected during parallel processing. The function ensures that important execution metrics are not lost when the shared memory segment is cleaned up.

The function calculates the size needed to store the SharedAggInfo structure plus all worker instrumentation data, allocates private memory for this data, and copies the entire structure from shared memory. This allows the execution statistics to be available for query planning feedback and performance analysis even after the parallel execution has finished.

## Parameters / Member Variables
- `node`: Pointer to the AggState structure containing the aggregate node with shared instrumentation information

## Dependencies
- Functions called/Symbols referenced:
  - palloc
  - memcpy
  - offsetof (macro)
- Data types referenced:
  - AggState
  - SharedAggInfo
  - AggregateInstrumentation
  - Size
- Called from (representative examples):
  - ExecParallelRetrieveInstrumentation

## Notes and Other Information
- This function only operates when shared_info is not NULL (i.e., when parallel execution was used)
- The function calculates the total size including all worker instrumentation data using offsetof and num_workers
- After copying, the shared_info pointer is updated to point to the private copy
- This is part of the cleanup process for parallel aggregate execution
- The instrumentation data includes performance metrics that can be used for query optimization
- Memory is allocated using palloc, making it subject to PostgreSQL's memory context management
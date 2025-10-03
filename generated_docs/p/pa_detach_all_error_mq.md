# pa_detach_all_error_mq

## Location
[src/backend/replication/logical/applyparallelworker.c:622-641](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L622-L641)

## Overview
Detaches error message queues from all parallel apply workers in the worker pool, ensuring proper cleanup during worker shutdown.

## Definition
```c
void pa_detach_all_error_mq(void)
```

## Detailed Description
This function iterates through all parallel apply workers in the ParallelApplyWorkerPool and detaches their error message queue handles. It's designed to be called during cleanup scenarios where all parallel apply workers need to be properly disconnected from their error communication channels. After detaching each error message queue, it sets the handle to NULL to prevent double-detachment issues.

The function ensures that no error message queue connections remain active when shutting down the logical replication infrastructure, which is crucial for proper resource cleanup and avoiding potential communication issues during shutdown sequences.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [shm_mq_detach](../s/shm_mq_detach.md) (detach from shared message queue)
  - ParallelApplyWorkerPool (global worker pool list)
  - [ParallelApplyWorkerInfo](../P/ParallelApplyWorkerInfo.md) (worker information structure)
- Called from (representative examples):
  - [logicalrep_worker_detach](../l/logicalrep_worker_detach.md)

## Notes and Other Information
- Function safely handles cases where error_mq_handle might be NULL
- Sets error_mq_handle to NULL after detachment to prevent double-detachment
- Part of the cleanup infrastructure for logical replication shutdown
- Public function (not static) indicating it's called from other modules
- Essential for proper resource management during worker pool shutdown
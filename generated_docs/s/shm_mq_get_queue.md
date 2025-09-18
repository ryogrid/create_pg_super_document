# shm_mq_get_queue

## Location
[src/backend/storage/ipc/shm_mq.c:905-913](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shm_mq.c#L905-L913)

## Overview
Retrieves the underlying shared memory message queue structure from a message queue handle, providing direct access to the queue for low-level operations.

## Definition
```c
shm_mq *shm_mq_get_queue(shm_mq_handle *mqh)
```

## Detailed Description
This simple accessor function extracts the shared memory message queue pointer from a handle structure. It provides a way to access the underlying queue structure directly when low-level operations or direct queue manipulation is required. The function is a straightforward getter that returns the mqh_queue field from the handle.

The function serves as an abstraction layer between the high-level handle interface and the low-level queue structure, allowing code that needs direct queue access to obtain it in a controlled manner while maintaining the handle-based API design.

## Parameters / Member Variables
- `mqh`: Handle to the shared memory message queue from which to extract the underlying queue pointer

## Dependencies
- Functions called/Symbols referenced:
  - [shm_mq_handle](shm_mq_handle.md) (parameter type)
  - [shm_mq](shm_mq.md) (return type)
- Called from (representative examples):
  - WaitForParallelWorkersToAttach
  - WaitForParallelWorkersToFinish

## Notes and Other Information
- Simple accessor function with no error checking or validation
- Provides direct access to shared memory queue structure for low-level operations
- Used primarily in parallel processing contexts where direct queue access is needed
- Part of the handle-based abstraction layer for shared memory message queues
- Returns raw pointer to shared memory structure - caller must ensure proper usage
- Essential for code that needs to interact with queue internals beyond the standard API
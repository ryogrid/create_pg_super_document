# attach_to_queues

## Location
[src/test/modules/test_shm_mq/worker.c:154-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_shm_mq/worker.c#L154-L175)

## Overview
Sets up input and output shared memory message queue handles for a worker process based on its worker number in a parallel processing pipeline.

## Definition
```c
static void attach_to_queues(dsm_segment *seg, shm_toc *toc, int myworkernumber,
                            shm_mq_handle **inqhp, shm_mq_handle **outqhp)
```

## Detailed Description
This function implements the queue attachment logic for the test_shm_mq parallel processing system. It establishes a pipeline pattern where:

1. **Queue Numbering**: Message queues are registered in the TOC at keys 1 through number-of-workers
2. **Pipeline Structure**: 
   - User backend writes to queue #1 and reads from queue #number-of-workers
   - Each worker reads from queue #myworkernumber and writes to queue #(myworkernumber+1)
   - This creates a processing pipeline where messages flow from user → worker1 → worker2 → ... → workerN → user

3. **Queue Setup Process**:
   - Looks up the input queue using the worker's number as the TOC key
   - Sets the current process as the receiver for the input queue
   - Attaches to the input queue to get a handle
   - Looks up the output queue (next worker in pipeline) using myworkernumber+1 as the key
   - Sets the current process as the sender for the output queue
   - Attaches to the output queue to get a handle

The function configures the shared memory message queues so that data flows through the worker pipeline in a controlled manner.

## Parameters / Member Variables
- `seg`: Pointer to the dynamic shared memory segment containing the message queues
- `toc`: Pointer to the shared memory table of contents for locating queue structures
- `myworkernumber`: The unique worker identifier (1-based) determining which queues to attach to
- `inqhp`: Output parameter receiving the handle to the input message queue (for receiving)
- `outqhp`: Output parameter receiving the handle to the output message queue (for sending)

## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_lookup](../s/shm_toc_lookup.md) (locate queue structures in TOC)
  - [shm_mq_set_receiver](../s/shm_mq_set_receiver.md) (configure receiving process for input queue)
  - [shm_mq_set_sender](../s/shm_mq_set_sender.md) (configure sending process for output queue)
  - [shm_mq_attach](../s/shm_mq_attach.md) (create handles for queue communication)
  - MyProc (current process descriptor)
- Called from (representative examples):
  - [test_shm_mq_main](../t/test_shm_mq_main.md) (during worker initialization)

## Notes and Other Information
- This function is static (internal to worker.c) and represents application-specific logic that would be customized for different use cases
- The queue numbering scheme ensures proper pipeline ordering and prevents deadlocks
- Both input and output queue handles are required for the worker to participate in the message passing pipeline
- The function assumes that the appropriate number of queues have been pre-allocated and registered in the TOC by the coordinator process
# ExecParallelSetupTupleQueues

## Location
[src/backend/executor/execParallel.c:535-586](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execParallel.c#L535-L586)

## Overview
ExecParallelSetupTupleQueues creates and configures shared memory message queues that enable parallel worker processes to return result tuples to the main backend process.

## Definition

```c
static shm_mq_handle **
ExecParallelSetupTupleQueues(ParallelContext *pcxt, bool reinitialize)
```
## Detailed Description
This static function establishes the communication infrastructure for parallel query execution by setting up tuple queues in shared memory. Each parallel worker process gets its own dedicated message queue to send result tuples back to the coordinator process.

The function operates in two modes:
1. **Initial setup** (reinitialize = false): Allocates new shared memory space for the queues and registers them in the shared memory table of contents
2. **Reinitialization** (reinitialize = true): Looks up previously allocated queue space for reuse

Key operations performed:
- Allocates an array of shared memory queue handles for tracking
- Either allocates or looks up shared memory space for the actual queues
- Creates individual message queues for each worker with fixed size (PARALLEL_TUPLE_QUEUE_SIZE)
- Sets the current process (MyProc) as the receiver for all queues
- Attaches to each queue to get handles for communication
- Registers the queue space in the shared memory table of contents for worker access

The function ensures that parallel workers have a reliable mechanism to stream result tuples back to the coordinating process without contention.

## Parameters / Member Variables
- : Parallel context structure containing:
  - : Number of parallel worker processes
  - : Shared memory table of contents for key-value storage
  - : Dynamic shared memory segment
- : Boolean flag indicating whether to allocate new queues (false) or reuse existing ones (true)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [shm_toc_allocate](../s/shm_toc_allocate.md) (allocate shared memory space)
  - [shm_toc_lookup](../s/shm_toc_lookup.md) (find existing shared memory space)
  - [shm_toc_insert](../s/shm_toc_insert.md) (register shared memory space)
  - [shm_mq_create](../s/shm_mq_create.md) (create message queue)
  - [shm_mq_set_receiver](../s/shm_mq_set_receiver.md) (set queue receiver process)
  - [shm_mq_attach](../s/shm_mq_attach.md) (attach to queue for communication)
  - [mul_size](../m/mul_size.md) (safe size multiplication)
  - PARALLEL_TUPLE_QUEUE_SIZE, PARALLEL_KEY_TUPLE_QUEUE (constants)
- Called from:
  - [ExecInitParallelPlan](ExecInitParallelPlan.md) (during initial parallel plan setup)
  - [ExecParallelReinitialize](ExecParallelReinitialize.md) (when reinitializing parallel execution)

## Notes and Other Information
- Returns NULL if no workers are configured (pcxt->nworkers == 0)
- Each worker gets a fixed-size message queue (PARALLEL_TUPLE_QUEUE_SIZE bytes)
- The main process becomes the receiver for all worker queues
- Queue handles are returned as an array for subsequent tuple reading operations
- The shared memory space is registered with PARALLEL_KEY_TUPLE_QUEUE for worker discovery
- This function is essential for establishing the data flow from workers back to the coordinator

## Simplified Source

```c
static shm_mq_handle **ExecParallelSetupTupleQueues(ParallelContext *pcxt, bool reinitialize)
{
    shm_mq_handle **responseq;
    char *tqueuespace;
    int i;

    // No setup needed if no workers
    if (pcxt->nworkers == 0)
        return NULL;

    // Allocate array of queue handles
    responseq = palloc(pcxt->nworkers * sizeof(shm_mq_handle *));

    // Get shared memory space for queues
    if (!reinitialize)
        // Allocate new space
        tqueuespace = shm_toc_allocate(pcxt->toc,
                                      pcxt->nworkers * PARALLEL_TUPLE_QUEUE_SIZE);
    else
        // Reuse existing space
        tqueuespace = shm_toc_lookup(pcxt->toc, PARALLEL_KEY_TUPLE_QUEUE, false);

    // Create one queue per worker
    for (i = 0; i < pcxt->nworkers; ++i)
    {
        shm_mq *mq;

        // Create queue at calculated offset
        mq = shm_mq_create(tqueuespace + (i * PARALLEL_TUPLE_QUEUE_SIZE),
                          PARALLEL_TUPLE_QUEUE_SIZE);

        // Set main process as receiver
        shm_mq_set_receiver(mq, MyProc);
        responseq[i] = shm_mq_attach(mq, pcxt->seg, NULL);
    }

    // Register queues for worker discovery
    if (!reinitialize)
        shm_toc_insert(pcxt->toc, PARALLEL_KEY_TUPLE_QUEUE, tqueuespace);

    return responseq;
}
```
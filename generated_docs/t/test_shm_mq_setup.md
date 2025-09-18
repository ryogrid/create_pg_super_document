# test_shm_mq_setup

## Location
src/test/modules/test_shm_mq/setup.c: 51 - 91

## Overview
This function sets up a dynamic shared memory segment and zero or more background workers for a shared memory message queue test run, providing the necessary infrastructure for inter-process communication testing.

## Definition
```c
void test_shm_mq_setup(int64 queue_size, int32 nworkers, dsm_segment **segp,
                       shm_mq_handle **output, shm_mq_handle **input)
```

## Detailed Description
The `test_shm_mq_setup` function orchestrates the complete setup process for testing shared memory message queues in PostgreSQL. It creates a dynamic shared memory segment, initializes the necessary data structures, spawns background worker processes, and establishes bidirectional communication channels through message queues. The function ensures that all worker processes are ready before returning, providing a synchronized starting point for test execution.

The setup process involves four main phases:
1. Creating and configuring a dynamic shared memory segment with message queues
2. Registering and starting background worker processes
3. Attaching message queue handles for communication
4. Synchronizing with workers to ensure they are ready for operation

## Parameters / Member Variables
- `queue_size`: Size in bytes for each message queue to be created in the shared memory segment
- `nworkers`: Number of background worker processes to spawn for the test
- `segp`: Output parameter returning a pointer to the created dynamic shared memory segment
- `output`: Output parameter returning a handle to the output message queue for sending data to workers
- `input`: Output parameter returning a handle to the input message queue for receiving data from workers

## Dependencies
- Functions called/Symbols referenced:
  - [setup_dynamic_shared_memory](../s/setup_dynamic_shared_memory.md)
  - [setup_background_workers](../s/setup_background_workers.md)
  - [shm_mq_attach](../s/shm_mq_attach.md)
  - [wait_for_workers_to_become_ready](../w/wait_for_workers_to_become_ready.md)
  - cancel_on_dsm_detach
  - [cleanup_background_workers](../c/cleanup_background_workers.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [test_shm_mq](test_shm_mq.md)
  - [test_shm_mq_pipelined](test_shm_mq_pipelined.md)

## Notes and Other Information
- This function is part of the PostgreSQL test infrastructure specifically designed for testing shared memory message queue functionality
- The function establishes a cleanup mechanism using `cancel_on_dsm_detach` to ensure proper resource cleanup when the dynamic shared memory segment is detached
- Worker processes are configured to automatically terminate when message queues shut down, eliminating the need for explicit cleanup after successful setup
- The function is located in `src/test/modules/test_shm_mq/setup.c:51-91`
- Memory allocated for worker state tracking is freed after setup completion since cleanup is handled through the DSM detach callback mechanism
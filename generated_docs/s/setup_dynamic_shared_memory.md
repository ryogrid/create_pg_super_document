# setup_dynamic_shared_memory

## Location
[src/test/modules/test_shm_mq/setup.c:92-174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_shm_mq/setup.c#L92-L174)

## Overview
This static function creates and configures a dynamic shared memory segment containing a control header and multiple message queues for shared memory message queue testing.

## Definition
```c
static void setup_dynamic_shared_memory(int64 queue_size, int nworkers,
                                         dsm_segment **segp, test_shm_mq_header **hdrp,
                                         shm_mq **outp, shm_mq **inp)
```

## Detailed Description
The `setup_dynamic_shared_memory` function creates a dynamic shared memory segment with a carefully structured layout for testing shared memory message queues. The segment consists of a small control region containing a `test_shm_mq_header` structure, followed by multiple message queue regions. The function creates `nworkers + 1` message queues: one for sending messages to workers (output queue) and `nworkers` queues for receiving messages back from workers, with the last queue designated as the input queue for the main process.

The function performs comprehensive validation of the queue size parameter, estimates the required shared memory size using the TOC (Table of Contents) estimator, creates the segment, and initializes all data structures. It sets up proper sender/receiver relationships for the message queues and maintains a synchronized header structure to track worker status.

## Parameters / Member Variables
- `queue_size`: Size in bytes for each message queue; must be at least `shm_mq_minimum_size` and fit in a `Size` type
- `nworkers`: Number of worker processes that will use the message queues
- `segp`: Output parameter returning a pointer to the created dynamic shared memory segment
- `hdrp`: Output parameter returning a pointer to the test header structure in shared memory
- `outp`: Output parameter returning a pointer to the output message queue (for sending to workers)
- `inp`: Output parameter returning a pointer to the input message queue (for receiving from workers)

## Dependencies
- Functions called/Symbols referenced:
  - shm_toc_initialize_estimator
  - shm_toc_estimate_chunk
  - shm_toc_estimate_keys
  - [shm_toc_estimate](shm_toc_estimate.md)
  - [dsm_create](../d/dsm_create.md)
  - [dsm_segment_address](../d/dsm_segment_address.md)
  - [shm_toc_create](shm_toc_create.md)
  - [shm_toc_allocate](shm_toc_allocate.md)
  - [shm_toc_insert](shm_toc_insert.md)
  - [shm_mq_create](shm_mq_create.md)
  - [shm_mq_set_sender](shm_mq_set_sender.md)
  - [shm_mq_set_receiver](shm_mq_set_receiver.md)
  - SpinLockInit
- Called from (representative examples):
  - [test_shm_mq_setup](../t/test_shm_mq_setup.md)

## Notes and Other Information
- This is a static function internal to the test_shm_mq module, located in `src/test/modules/test_shm_mq/setup.c:92-174`
- The function validates queue size parameters and reports errors for invalid values (too small or causing overflow)
- Uses a Table of Contents (TOC) approach to organize the shared memory layout, allowing for proper alignment and padding
- The magic number `PG_TEST_SHM_MQ_MAGIC` is used to identify the shared memory segment type
- The header structure includes spinlock-protected counters for tracking worker attachment and readiness states
- Message queue 0 is designated for output (main process sends to workers), and the last queue is for input (workers send to main process)
- The function ensures proper sender/receiver role assignment using `MyProc` for the main process
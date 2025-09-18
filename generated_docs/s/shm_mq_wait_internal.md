# shm_mq_wait_internal

## Location
src/backend/storage/ipc/shm_mq.c: 1218 - 1269

## Overview
Waits for a counterparty process to attach to a shared message queue by monitoring a specific memory pointer until it becomes non-NULL.

## Definition
```c
static bool shm_mq_wait_internal(shm_mq *mq, PGPROC **ptr, BackgroundWorkerHandle *handle)
```

## Detailed Description
This function implements the core waiting mechanism for queue attachment synchronization. It continuously polls a specified memory pointer (typically mq_sender or mq_receiver) that becomes non-NULL when the counterparty process attaches to the queue. The function uses spinlocks for atomic pointer checks and latch-based waiting to avoid busy polling. It can monitor background worker processes for unexpected death and will exit early if the queue becomes detached or the worker terminates. The function is critical for establishing proper bidirectional communication channels in shared message queues.

## Parameters / Member Variables
- `mq`: Pointer to the shared message queue structure
- `ptr`: Pointer to memory address expected to become non-NULL when counterparty attaches (usually &mq->mq_sender or &mq->mq_receiver)
- `handle`: Background worker handle for the counterparty process (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - GetBackgroundWorkerPid
  - WaitLatch
  - ResetLatch
  - CHECK_FOR_INTERRUPTS
- Called from (representative examples):
  - shm_mq_receive
  - shm_mq_wait_for_attach
  - shm_mq_send_bytes

## Notes and Other Information
- Uses spinlocks to atomically check the attachment pointer without data races
- Implements latch-based waiting to avoid consuming CPU while waiting for attachment
- Returns false if the queue is detached or the background worker dies unexpectedly
- Can potentially wait indefinitely if handle is NULL and the counterparty never attaches
- Checks for interrupts during waiting to allow for cancellation
- Critical for synchronizing sender and receiver attachment in parallel query execution
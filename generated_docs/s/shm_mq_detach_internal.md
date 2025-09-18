# shm_mq_detach_internal

## Location
src/backend/storage/ipc/shm_mq.c: 882 - 904

## Overview
Notifies the counterpart process that detachment from a shared memory message queue is occurring, ensuring the other process doesn't block indefinitely waiting for communication.

## Definition
```c
static void shm_mq_detach_internal(shm_mq *mq)
```

## Detailed Description
This internal function provides the core mechanism for notifying a counterpart process about queue detachment. It performs atomic operations under spinlock protection to set the detachment flag and wake up the waiting process. The function determines which process (sender or receiver) is the counterpart by comparing against MyProc, then sets the detachment flag and signals the counterpart's process latch.

The function is designed to be safe for use in cleanup callbacks where the local handle might have already been freed. It operates directly on the shared memory queue structure and uses minimal local state, making it suitable for error recovery scenarios and dynamic shared memory segment cleanup callbacks.

## Parameters / Member Variables
- `mq`: Pointer to the shared memory message queue structure to mark as detached

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease  
  - [SetLatch](../S/SetLatch.md)
  - MyProc (global variable)
- Called from (representative examples):
  - [shm_mq_detach](shm_mq_detach.md)
  - [shm_mq_detach_callback](shm_mq_detach_callback.md)

## Notes and Other Information
- Operates under spinlock protection to ensure atomic updates of queue state
- Sets mq_detached flag to true, causing future operations to return SHM_MQ_DETACHED
- Wakes up counterpart process via SetLatch to prevent indefinite blocking
- Safe for use in DSM cleanup callbacks where local handles may be freed
- When sender detaches, receiver can read remaining messages before getting SHM_MQ_DETACHED
- When receiver detaches, further send attempts return SHM_MQ_DETACHED immediately
- Critical for preventing deadlocks in PostgreSQL's parallel processing architecture
# shm_mq_set_receiver

## Location
[src/backend/storage/ipc/shm_mq.c:206-223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shm_mq.c#L206-L223)

## Overview
Sets the identity of the process that will receive messages from a shared message queue and signals the sender if already attached.

## Definition
void shm_mq_set_receiver(shm_mq *mq, PGPROC *proc)

## Detailed Description
The shm_mq_set_receiver function assigns a PGPROC structure (representing a PostgreSQL process) as the receiver for the specified shared message queue. It uses the queue mutex for thread-safe assignment and ensures that a receiver can only be set once (via assertion). If a sender is already attached to the queue, the function signals the sender process by setting its latch, which can wake up the sender if it was waiting for a receiver to be attached.

## Parameters / Member Variables
- mq: Pointer to the shared message queue structure
- proc: Pointer to the PGPROC structure representing the receiving process

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - [SetLatch](../S/SetLatch.md)
  - [PGPROC](../P/PGPROC.md) (structure)
- Called from (representative examples):
  - Functions that set up parallel worker processes
  - Functions that establish inter-process communication channels

## Notes and Other Information
- The receiver can only be set once per queue (enforced by assertion)
- Uses mutex protection to ensure thread-safe access to mq_receiver and mq_sender fields
- Automatically signals the sender process if one is already attached
- The latch mechanism allows for efficient process synchronization
- Located in src/backend/storage/ipc/shm_mq.c:206-223
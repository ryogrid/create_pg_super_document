# shm_mq_set_sender

## Location
src/backend/storage/ipc/shm_mq.c: 224 - 241

## Overview
Sets the identity of the process that will send messages to a shared message queue and signals the receiver if already attached.

## Definition
void shm_mq_set_sender(shm_mq *mq, PGPROC *proc)

## Detailed Description
The shm_mq_set_sender function assigns a PGPROC structure (representing a PostgreSQL process) as the sender for the specified shared message queue. It uses the queue mutex for thread-safe assignment and ensures that a sender can only be set once (via assertion). If a receiver is already attached to the queue, the function signals the receiver process by setting its latch, which can wake up the receiver if it was waiting for a sender to be attached.

## Parameters / Member Variables
- mq: Pointer to the shared message queue structure
- proc: Pointer to the PGPROC structure representing the sending process

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - SetLatch
  - PGPROC (structure)
- Called from (representative examples):
  - Functions that set up parallel worker processes
  - Functions that establish inter-process communication channels

## Notes and Other Information
- The sender can only be set once per queue (enforced by assertion)
- Uses mutex protection to ensure thread-safe access to mq_sender and mq_receiver fields
- Automatically signals the receiver process if one is already attached
- The latch mechanism allows for efficient process synchronization
- Mirrors the functionality of shm_mq_set_receiver but for the sending side
- Located in src/backend/storage/ipc/shm_mq.c:224-241
# shm_mq_get_sender

## Location
[src/backend/storage/ipc/shm_mq.c:257-289](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shm_mq.c#L257-L289)

## Overview
Retrieves the currently configured sender process from a shared message queue in a thread-safe manner.

## Definition
PGPROC *shm_mq_get_sender(shm_mq *mq)

## Detailed Description
The shm_mq_get_sender function returns a pointer to the PGPROC structure representing the process currently configured as the sender for the specified shared message queue. It uses mutex protection to ensure thread-safe access to the mq_sender field, preventing race conditions when multiple processes might be accessing the queue configuration simultaneously.

## Parameters / Member Variables
- mq: Pointer to the shared message queue structure

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - [PGPROC](../P/PGPROC.md) (structure)
- Called from (representative examples):
  - Functions that need to check queue configuration
  - Process management and monitoring functions

## Notes and Other Information
- Returns NULL if no sender has been configured yet
- Uses mutex protection for thread-safe access to the sender field
- The returned PGPROC pointer should not be modified by the caller
- Provides read-only access to the queue sender configuration
- Mirrors the functionality of shm_mq_get_receiver but for the sending side
- Located in src/backend/storage/ipc/shm_mq.c:257-289
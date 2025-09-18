# shm_mq_get_receiver

## Location
src/backend/storage/ipc/shm_mq.c: 242 - 256

## Overview
Retrieves the currently configured receiver process from a shared message queue in a thread-safe manner.

## Definition
PGPROC *shm_mq_get_receiver(shm_mq *mq)

## Detailed Description
The shm_mq_get_receiver function returns a pointer to the PGPROC structure representing the process currently configured as the receiver for the specified shared message queue. It uses mutex protection to ensure thread-safe access to the mq_receiver field, preventing race conditions when multiple processes might be accessing the queue configuration simultaneously.

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
- Returns NULL if no receiver has been configured yet
- Uses mutex protection for thread-safe access to the receiver field
- The returned PGPROC pointer should not be modified by the caller
- Provides read-only access to the queue receiver configuration
- Located in src/backend/storage/ipc/shm_mq.c:242-256
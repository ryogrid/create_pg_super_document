# shm_mq_wait_for_attach

## Location
src/backend/storage/ipc/shm_mq.c: 820 - 842

## Overview
Waits for the other process that is supposed to use a shared memory message queue to attach to it, providing synchronization between sender and receiver processes.

## Definition
```c
shm_mq_result shm_mq_wait_for_attach(shm_mq_handle *mqh)
```

## Detailed Description
This function provides a blocking wait mechanism for inter-process synchronization in PostgreSQL's shared memory message queue system. It determines whether the calling process is the sender or receiver by checking against MyProc, then waits for the counterpart process to attach to the queue. The function leverages the internal waiting mechanism to detect either successful attachment or process death/detachment.

The function performs role detection by comparing the current process (MyProc) with the queue's sender and receiver fields. Based on this comparison, it determines which process (victim) it should wait for and delegates the actual waiting logic to shm_mq_wait_internal.

## Parameters / Member Variables
- `mqh`: Handle to the shared memory message queue containing the queue pointer and optional background worker handle for process death detection

## Dependencies
- Functions called/Symbols referenced:
  - [shm_mq_get_receiver](shm_mq_get_receiver.md)
  - [shm_mq_get_sender](shm_mq_get_sender.md)  
  - [shm_mq_wait_internal](shm_mq_wait_internal.md)
  - SHM_MQ_SUCCESS
  - SHM_MQ_DETACHED
- Called from (representative examples):
  - Referenced in shm_mq.h header file

## Notes and Other Information
- Returns SHM_MQ_DETACHED if the worker has already detached or dies during the wait
- Returns SHM_MQ_SUCCESS if successful attachment is detected
- Process death detection is only possible if a background worker handle was passed to shm_mq_attach()
- Uses assertion to ensure the calling process is either the designated sender or receiver
- Critical for establishing reliable communication channels in PostgreSQL's parallel processing framework
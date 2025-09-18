# shm_mq_counterparty_gone

## Location
src/backend/storage/ipc/shm_mq.c: 1179 - 1217

## Overview
Tests whether the counterparty (sender or receiver) of a shared message queue has definitively terminated or become unavailable.

## Definition
```c
static bool shm_mq_counterparty_gone(shm_mq *mq, BackgroundWorkerHandle *handle)
```

## Detailed Description
This function determines if the counterparty process for a shared message queue has terminated or is no longer available for communication. It checks two primary indicators: whether the queue has been explicitly detached (mq_detached flag), and if a background worker handle is provided, whether the associated worker process has died or failed to start. When a worker is found to be dead, the function marks the queue as detached to make the termination official and prevent further communication attempts.

## Parameters / Member Variables
- `mq`: Pointer to the shared message queue structure to check
- `handle`: Background worker handle for the counterparty process (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [GetBackgroundWorkerPid](../G/GetBackgroundWorkerPid.md)
  - BGWH_STARTED
  - BGWH_NOT_YET_STARTED
- Called from (representative examples):
  - [shm_mq_receive](shm_mq_receive.md)
  - [shm_mq_send_bytes](shm_mq_send_bytes.md)

## Notes and Other Information
- Returns true immediately if the queue is already marked as detached
- When a background worker handle is provided, queries the worker's status to detect premature termination
- Automatically marks the queue as detached when worker death is detected
- Used to avoid indefinite waiting when the counterparty process has already terminated
- Handles the case where a worker may not have started yet (BGWH_NOT_YET_STARTED is considered valid)
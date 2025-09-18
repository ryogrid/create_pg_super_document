# pq_cleanup_redirect_to_shm_mq

## Location
src/backend/libpq/pqmq.c: 67 - 77

## Overview
Cleanup function that resets the shared memory message queue communication state when the associated dynamic shared memory segment is detached.

## Definition
```c
static void pq_cleanup_redirect_to_shm_mq(dsm_segment *seg, Datum arg)
```

## Detailed Description
This function serves as a cleanup callback that is automatically invoked when a dynamic shared memory segment containing a shared memory message queue is detached. It ensures proper cleanup by nullifying the message queue handle and setting the output destination to DestNone, effectively disabling further message sending attempts through the now-invalid queue.

This cleanup is critical for preventing access to freed or invalid shared memory resources after a parallel worker terminates or when the DSM segment is no longer available.

## Parameters / Member Variables
- `seg`: The dynamic shared memory segment being detached (unused in implementation)
- `arg`: Additional argument passed to the callback (unused in implementation)

## Dependencies
- Functions called/Symbols referenced:
  - DestNone
  - pq_mq_handle (global variable)
  - whereToSendOutput (global variable)
- Called from (representative examples):
  - pq_redirect_to_shm_mq (registered as DSM detach callback)

## Notes and Other Information
- This is a static function, only accessible within the pqmq.c file
- Automatically called by the DSM management system when segments are detached
- Sets pq_mq_handle to NULL to prevent further use of the invalid handle
- Changes whereToSendOutput to DestNone to stop message routing
- Essential for proper resource cleanup in parallel processing scenarios
- Does not perform explicit error handling as it's a cleanup function
# cleanup_background_workers

## Location
src/test/modules/test_shm_mq/setup.c: 246 - 257

## Overview
This static function serves as a cleanup callback to terminate all registered background worker processes when the dynamic shared memory segment is detached.

## Definition
```c
static void cleanup_background_workers(dsm_segment *seg, Datum arg)
```

## Detailed Description
The `cleanup_background_workers` function is designed as a callback for dynamic shared memory (DSM) detachment events. When the shared memory segment is being destroyed or detached, this function ensures that all associated background worker processes are properly terminated. It iterates through all worker handles stored in the `worker_state` structure and systematically terminates each worker using the PostgreSQL background worker management system.

This function is critical for preventing orphaned worker processes that could continue running after the test infrastructure is torn down. It provides a fail-safe mechanism to ensure clean resource cleanup even in error scenarios where normal shutdown procedures might not execute properly.

## Parameters / Member Variables
- `seg`: Pointer to the dynamic shared memory segment being detached (required by DSM callback signature but not used in implementation)
- `arg`: Datum containing a pointer to the `worker_state` structure that tracks all registered workers

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetPointer
  - TerminateBackgroundWorker
- Called from (representative examples):
  - test_shm_mq_setup (via cancel_on_dsm_detach)
  - setup_background_workers (via on_dsm_detach)

## Notes and Other Information
- This is a static function internal to the test_shm_mq module, located in `src/test/modules/test_shm_mq/setup.c:246-257`
- Registered as a callback using `on_dsm_detach` in `setup_background_workers` for early cleanup scenarios
- Also registered with `cancel_on_dsm_detach` in `test_shm_mq_setup` after successful worker initialization
- The function properly handles the worker count by decrementing `nworkers` before termination to maintain consistency
- Uses reverse iteration (decrementing index) to terminate workers, which is safe for array-based worker handle storage
- Essential for preventing resource leaks and zombie processes in test scenarios
- The DSM segment parameter follows the standard callback signature but is not utilized in the cleanup logic
- Designed to be idempotent - can be called multiple times safely without adverse effects
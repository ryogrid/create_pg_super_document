# wait_for_workers_to_become_ready

## Location
src/test/modules/test_shm_mq/setup.c: 258 - 305

## Overview
This static function synchronously waits for all background worker processes to complete their initialization and become ready for message queue operations, providing robust error handling and status monitoring.

## Definition
```c
static void wait_for_workers_to_become_ready(worker_state *wstate,
                                              volatile test_shm_mq_header *hdr)
```

## Detailed Description
The `wait_for_workers_to_become_ready` function implements a synchronization mechanism that ensures all background worker processes have successfully initialized and are ready to participate in shared memory message queue operations. It continuously monitors the shared memory header's `workers_ready` counter while also checking the health status of all worker processes.

The function uses a polling loop with efficient waiting mechanisms. It employs PostgreSQL's latch system to sleep until signaled, rather than busy-waiting, which reduces CPU overhead. The function also registers a custom wait event for observability and monitoring purposes. It performs comprehensive health checks on each iteration, ensuring that worker failures are detected promptly.

The synchronization logic is critical for test reliability - it prevents the main process from proceeding with message queue operations before all workers are fully operational, avoiding race conditions and ensuring predictable test behavior.

## Parameters / Member Variables
- `wstate`: Pointer to worker state structure containing handles and count of all registered background workers
- `hdr`: Volatile pointer to the shared memory header structure that tracks worker readiness status

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - check_worker_status
  - WaitEventExtensionNew
  - WaitLatch
  - ResetLatch
  - CHECK_FOR_INTERRUPTS
  - ereport
- Called from (representative examples):
  - test_shm_mq_setup

## Notes and Other Information
- This is a static function internal to the test_shm_mq module, located in `src/test/modules/test_shm_mq/setup.c:258-305`
- Uses spinlock protection when accessing the shared `workers_ready` counter to ensure atomic reads
- Implements efficient waiting using PostgreSQL's latch mechanism with `WL_LATCH_SET | WL_EXIT_ON_PM_DEATH` flags
- Creates a custom wait event "TestShmMqBgWorkerStartup" for monitoring and debugging purposes using `we_bgworker_startup` static variable
- Continuously monitors worker health using `check_worker_status` to detect premature worker termination
- Handles interrupts properly by calling `CHECK_FOR_INTERRUPTS()` after each wait cycle
- Reports detailed error information when workers fail to start, helping with diagnosis of resource or configuration issues
- The function will exit on postmaster death (`WL_EXIT_ON_PM_DEATH`) to prevent orphaned processes
- Uses `ResetLatch` to prevent spinning in subsequent wait cycles
- The volatile qualifier on the header parameter ensures proper memory ordering for shared memory access
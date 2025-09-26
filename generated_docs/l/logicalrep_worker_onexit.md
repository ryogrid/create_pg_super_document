# logicalrep_worker_onexit

## Location
src/backend/replication/logical/launcher.c: 831 - 860

## Overview
A comprehensive cleanup function registered as an exit handler for logical replication worker processes that performs graceful disconnection, resource cleanup, and state management upon worker termination.

## Definition


## Detailed Description
This static function serves as a critical exit handler for logical replication worker processes, ensuring proper cleanup of all resources and connections when a worker terminates. The function is designed to handle both normal and abnormal termination scenarios, performing a comprehensive cleanup sequence.

The cleanup process includes several important steps:
1. **Connection Cleanup**: Gracefully disconnects from the remote database if a WAL receiver connection exists
2. **Worker Detachment**: Calls logicalrep_worker_detach() to properly detach from the worker slot and stop any parallel workers
3. **File Resource Cleanup**: Removes streaming transaction filesets if they exist
4. **Lock Release**: Releases all session-level locks that might have been acquired outside of transactions (particularly important in parallel apply mode)
5. **Launcher Notification**: Wakes up the apply launcher to potentially restart the worker or handle the termination

The function includes special handling for parallel apply workers, where session-level locks may be acquired outside of transactions and wouldn't normally be released on worker termination.

## Parameters / Member Variables
- : Exit code of the terminating process (standard exit handler parameter, not used in this implementation)
- : Datum argument passed to the exit handler (standard parameter, not used in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - walrcv_disconnect
  - logicalrep_worker_detach
  - FileSetDeleteAll
  - LockReleaseAll
  - ApplyLauncherWakeup
  - DEFAULT_LOCKMETHOD
- Called from (representative examples):
  - logicalrep_worker_attach (src/backend/replication/logical/launcher.c:747)

## Notes and Other Information
- This is a static function, only accessible within the launcher.c file
- The function follows the standard PostgreSQL exit handler signature (int code, Datum arg)
- Registered via before_shmem_exit() during worker attachment to ensure it's called during process shutdown
- The function handles special cases for parallel apply workers where locks may persist beyond transaction boundaries
- InitializingApplyWorker flag is checked to avoid releasing locks during worker initialization phase
- Waking up the launcher ensures that the system can respond appropriately to worker termination (e.g., restarting failed workers)
- The comprehensive cleanup prevents resource leaks and ensures system consistency after worker termination
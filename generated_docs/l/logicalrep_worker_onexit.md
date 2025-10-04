# logicalrep_worker_onexit

## Location
[src/backend/replication/logical/launcher.c:831-860](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L831-L860)

## Overview
A comprehensive cleanup function registered as an exit handler for logical replication worker processes that performs graceful disconnection, resource cleanup, and state management upon worker termination.

## Definition

```c
static void
logicalrep_worker_onexit(int code, Datum arg)
```
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
- `code`: Exit code of the terminating process (standard exit handler parameter, not used in this implementation)
- `arg`: Datum argument passed to the exit handler (standard parameter, not used in this implementation)
## Dependencies
- Functions called/Symbols referenced:
  - walrcv_disconnect
  - [logicalrep_worker_detach](logicalrep_worker_detach.md)
  - [FileSetDeleteAll](../F/FileSetDeleteAll.md)
  - [LockReleaseAll](../L/LockReleaseAll.md)
  - [ApplyLauncherWakeup](../A/ApplyLauncherWakeup.md)
  - DEFAULT_LOCKMETHOD
- Called from (representative examples):
  - [logicalrep_worker_attach](logicalrep_worker_attach.md) (src/backend/replication/logical/launcher.c:747)

## Notes and Other Information
- This is a static function, only accessible within the launcher.c file
- The function follows the standard PostgreSQL exit handler signature (int code, Datum arg)
- Registered via before_shmem_exit() during worker attachment to ensure it's called during process shutdown
- The function handles special cases for parallel apply workers where locks may persist beyond transaction boundaries
- InitializingApplyWorker flag is checked to avoid releasing locks during worker initialization phase
- Waking up the launcher ensures that the system can respond appropriately to worker termination (e.g., restarting failed workers)
- The comprehensive cleanup prevents resource leaks and ensures system consistency after worker termination

## Simplified Source

```c
static void logicalrep_worker_onexit(int code, Datum arg)
{
    // Step 1: Disconnect gracefully from remote database
    if (LogRepWorkerWalRcvConn)
        walrcv_disconnect(LogRepWorkerWalRcvConn);

    // Step 2: Detach from worker slot and stop parallel workers
    logicalrep_worker_detach();

    // Step 3: Clean up streaming transaction files
    if (MyLogicalRepWorker->stream_fileset != NULL)
        FileSetDeleteAll(MyLogicalRepWorker->stream_fileset);

    // Step 4: Release session-level locks (important for parallel apply)
    // Locks may be acquired outside transactions and need manual release
    if (!InitializingApplyWorker)
        LockReleaseAll(DEFAULT_LOCKMETHOD, true);

    // Step 5: Notify launcher about worker termination
    ApplyLauncherWakeup();
}
```
# Exec_ListenPreCommit

## Location
[src/backend/commands/async.c:1041-1135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L1041-L1135)

## Overview
Prepares a backend process to receive notifications by registering it as a listener in the shared notification queue during the pre-commit phase of transaction processing.

## Definition

```c
static void
Exec_ListenPreCommit(void)
```
## Detailed Description
This function is called during the pre-commit phase to ensure that a backend process is ready to catch any incoming NOTIFY messages. It performs the following key operations:

1. **Registration Check**: Returns early if the process is already registered as a listener or has already run in the current transaction
2. **Exit Handler Setup**: Registers an exit handler () to ensure proper cleanup when the backend terminates
3. **Queue Position Initialization**: Sets the backend's position in the notification queue to avoid missing notifications while optimizing startup time by adopting the maximum position of other backends in the same database
4. **Listener List Management**: Inserts the backend into the linked list of active listeners in the correct position based on ProcNumber
5. **Queue Advancement**: Moves the position forward past already-committed notifications to avoid processing stale messages

The function uses exclusive locking on  to safely manipulate the shared queue structures and listener list.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  -  - Registers cleanup handler
  -  - Cleanup function for process termination
  -  - Shared memory locking
  -  - Advances queue position past committed notifications
  - Various queue macros: , , , etc.

- Called from:
  -  (src/backend/commands/async.c:881) - Main pre-commit notification handler

## Notes and Other Information
- This function is part of PostgreSQL's LISTEN/NOTIFY asynchronous messaging system
- It only runs once per backend process and only when the process first starts listening
- The function carefully positions the backend in the notification queue to balance between not missing notifications and not processing unnecessary old notifications
- Debug tracing is available when  is enabled
- The registration as a listener persists for the lifetime of the backend process

## Simplified Source

```c
static void
Exec_ListenPreCommit(void)
{
    QueuePosition head;
    QueuePosition max;
    ProcNumber prevListener;

    // Skip if already registered or already ran this transaction
    if (amRegisteredListener)
        return;

    // Debug logging if tracing enabled
    if (Trace_notify)
        elog(DEBUG1, "Exec_ListenPreCommit(%d)", MyProcPid);

    // Register exit handler to ensure cleanup on backend termination
    if (!unlistenExitRegistered)
    {
        before_shmem_exit(Async_UnlistenOnExit, 0);
        unlistenExitRegistered = true;
    }

    // Initialize queue position - need exclusive lock for safety
    LWLockAcquire(NotifyQueueLock, LW_EXCLUSIVE);

    head = QUEUE_HEAD;
    max = QUEUE_TAIL;
    prevListener = INVALID_PROC_NUMBER;

    // Optimize startup by adopting max position from other backends in same DB
    for (ProcNumber i = QUEUE_FIRST_LISTENER; i != INVALID_PROC_NUMBER; i = QUEUE_NEXT_LISTENER(i))
    {
        if (QUEUE_BACKEND_DBOID(i) == MyDatabaseId)
            max = QUEUE_POS_MAX(max, QUEUE_BACKEND_POS(i));

        // Find insertion point in listener list
        if (i < MyProcNumber)
            prevListener = i;
    }

    // Set backend info in shared queue
    QUEUE_BACKEND_POS(MyProcNumber) = max;
    QUEUE_BACKEND_PID(MyProcNumber) = MyProcPid;
    QUEUE_BACKEND_DBOID(MyProcNumber) = MyDatabaseId;

    // Insert into listener list at correct position
    if (prevListener != INVALID_PROC_NUMBER)
    {
        QUEUE_NEXT_LISTENER(MyProcNumber) = QUEUE_NEXT_LISTENER(prevListener);
        QUEUE_NEXT_LISTENER(prevListener) = MyProcNumber;
    }
    else
    {
        QUEUE_NEXT_LISTENER(MyProcNumber) = QUEUE_FIRST_LISTENER;
        QUEUE_FIRST_LISTENER = MyProcNumber;
    }

    LWLockRelease(NotifyQueueLock);

    // Mark as registered listener
    amRegisteredListener = true;

    // Skip over already-committed notifications
    if (!QUEUE_POS_EQUAL(max, head))
        asyncQueueReadAllNotifications();
}
```
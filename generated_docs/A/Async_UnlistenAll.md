# Async_UnlistenAll

## Location
[src/backend/commands/async.c:770-789](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L770-L789)

## Overview
Executes the SQL UNLISTEN * command and is also invoked at backend exit to remove all listen subscriptions for the current session.

## Definition

```c
void
Async_UnlistenAll(void)
```
## Detailed Description
Async_UnlistenAll handles the UNLISTEN * command which removes all notification channel subscriptions for the current session. It also serves as a cleanup function called during backend exit to ensure proper resource cleanup. The function provides a wrapper around queue_listen with the LISTEN_UNLISTEN_ALL action. Like Async_Unlisten, it includes an optimization to avoid unnecessary work when the session could not possibly be listening to any channels. The actual unlisten operation is deferred until transaction commit to maintain transactional semantics.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - elog (for debug logging)
  - [queue_listen](../q/queue_listen.md)
  - LISTEN_UNLISTEN_ALL (enum constant)
  - DEBUG1 (logging level)
- Called from (representative examples):
  - [DiscardAll](../D/DiscardAll.md)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- Dual purpose: handles UNLISTEN * command and backend exit cleanup
- Includes optimization to skip queueing when no listening is possible
- Checks both pendingActions and unlistenExitRegistered flags for optimization
- Passes empty string as channel parameter to queue_listen (ignored for UNLISTEN_ALL)
- Provides debug logging with process ID when Trace_notify is enabled
- Defers actual listen list clearing until transaction commit
- Part of PostgreSQL's asynchronous notification system cleanup mechanism
- Public interface function declared in async.h header

## Simplified Source

```c
void Async_UnlistenAll(void) {
    // Optional debug logging
    if (Trace_notify)
        elog(DEBUG1, "Async_UnlistenAll(%d)", MyProcPid);

    // Optimization: skip if we couldn't possibly be listening
    if (pendingActions == NULL && !unlistenExitRegistered)
        return;

    // Queue the unlisten-all operation for transaction commit
    queue_listen(LISTEN_UNLISTEN_ALL, "");
}
```
# Exec_UnlistenAllCommit

## Location
[src/backend/commands/async.c:1194-1211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L1194-L1211)

## Overview
Removes all channels from the backend's listen list during the commit phase of an UNLISTEN * command or during backend cleanup.

## Definition

```c
static void
Exec_UnlistenAllCommit(void)
```
## Detailed Description
This function is called to completely clear the backend's list of listened channels. It is used during the commit phase of an "UNLISTEN *" command or during backend process cleanup. The function performs the following operations:

1. **Debug Logging**: If  is enabled, logs the unlisten-all operation with the process ID
2. **Deep List Cleanup**: Uses  to free both the list structure and all the channel name strings it contains
3. **List Reset**: Sets  to  to indicate no channels are being listened to

This is a simple but comprehensive cleanup function that ensures all memory associated with channel listening is properly freed.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  -  - Logging function for debug output
  -  - Deep cleanup function that frees list and all contained data
  -  - Constant representing an empty list

- Called from:
  -  (src/backend/commands/async.c:998) - Main commit-time notification handler
  -  (src/backend/commands/async.c:825) - [Backend](../B/Backend.md) exit cleanup handler

## Notes and Other Information
- This function is part of PostgreSQL's LISTEN/NOTIFY asynchronous messaging system
- The function serves dual purposes: executing "UNLISTEN *" commands and cleanup during backend termination
-  is used instead of  because the channel names (strings) also need to be freed
- Debug tracing is available when  is enabled
- After this function completes, the backend is no longer listening to any notification channels
- The function is called by  to ensure proper cleanup when a backend process terminates

## Simplified Source

```c
static void
Exec_UnlistenAllCommit(void)
{
    // Debug logging if tracing is enabled
    if (Trace_notify)
        elog(DEBUG1, "Exec_UnlistenAllCommit(%d)", MyProcPid);

    // Free all listen channels and reset list
    list_free_deep(listenChannels);
    listenChannels = NIL;
}
```
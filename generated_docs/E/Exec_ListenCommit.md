# Exec_ListenCommit

## Location
[src/backend/commands/async.c:1136-1162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L1136-L1162)

## Overview
Adds a channel name to the list of channels the backend process is actively listening on during the commit phase of LISTEN command processing.

## Definition

```c
static void
Exec_ListenCommit(const char *channel)
```
## Detailed Description
This function is called during the commit phase of a LISTEN command to officially register a channel name in the backend's list of listened channels. The function performs the following operations:

1. **Duplicate Check**: Uses  to verify the channel isn't already being listened to, avoiding duplicate entries
2. **Memory Management**: Switches to  to ensure the channel name persists beyond the current transaction
3. **List Update**: Appends a duplicate of the channel name to the global  list using  and 

The function includes a note about potential out-of-memory conditions occurring after commit, which could theoretically cause issues but are currently not guarded against.

## Parameters / Member Variables
- : The name of the notification channel to start listening on (null-terminated string)

## Dependencies
- Functions called/Symbols referenced:
  -  - Checks if already listening on the specified channel
  -  - Memory context management
  -  - [List](../L/List.md) manipulation function
  -  - [String](../S/String.md) duplication function

- Called from:
  -  (src/backend/commands/async.c:992) - Main commit-time notification handler

## Notes and Other Information
- This function is part of PostgreSQL's LISTEN/NOTIFY asynchronous messaging system
- The function operates during transaction commit, after the pre-commit phase handled by 
- Channel names are stored in  to persist for the lifetime of the backend process
- The comment indicates a known theoretical vulnerability to out-of-memory errors post-commit
- This function works in conjunction with  to complete the LISTEN command processing
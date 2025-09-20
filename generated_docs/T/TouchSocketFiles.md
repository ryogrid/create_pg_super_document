# TouchSocketFiles

## Location
[src/backend/libpq/pqcomm.c:829-846](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L829-L846)

## Overview
Updates the modification time of all PostgreSQL socket files to prevent them from being removed by system cleanup processes.

## Definition

```c
void
TouchSocketFiles(void)
```
## Detailed Description
This function iterates through all PostgreSQL socket files and updates their modification time using the utime() system call. This maintenance operation is necessary because normal socket operations typically don't update the file's modification time, which can lead to automatic cleanup daemons (such as /tmp directory cleaners) removing the socket files due to their apparent inactivity.

The function operates on a global list (sock_paths) that contains paths to all socket files created by the PostgreSQL server. It deliberately ignores any errors that occur during the utime() operations, as failure to update modification times is not critical to server operation.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - ListCell (PostgreSQL list cell type)
  - foreach (PostgreSQL list iteration macro) 
  - lfirst (PostgreSQL list access macro)
  - utime (system call to update file timestamps)
  - sock_paths (global list variable containing socket file paths)

- Called from (representative examples):
  - [ServerLoop](../S/ServerLoop.md) (periodically called during normal server operation to maintain socket files)

## Notes and Other Information
- This function was created as a workaround for the design decision to place socket files in /tmp directory
- The comment suggests that placing socket files in /tmp was not ideal from a design perspective
- Errors from utime() calls are deliberately ignored since timestamp updates are not critical
- The function prevents overly aggressive system cleanup processes from removing active socket files
- Should be called periodically during server operation to maintain socket file visibility to the system
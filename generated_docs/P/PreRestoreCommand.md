# PreRestoreCommand

## Location
src/backend/postmaster/startup.c: 268 - 281

## Overview
PreRestoreCommand is a function that prepares the startup process for entering a restore command execution state, setting appropriate flags and handling pending shutdown requests safely.

## Definition
```c
void PreRestoreCommand(void)
```

## Detailed Description
PreRestoreCommand is called before executing a restore command (typically for archive recovery) to establish a safe execution context. The function sets the global flag in_restore_command to true, which signals to the process's signal handlers that the startup process is in a state where it can safely exit immediately upon receiving a SIGTERM signal. This is important because restore commands represent safe points in the recovery process where interruption won't compromise data consistency.

The function also performs a critical safety check by examining the shutdown_requested flag. If a shutdown was already requested before entering this function, it immediately exits with code 1, ensuring that shutdown requests are not missed due to timing issues between signal delivery and entering the restore command state.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - proc_exit (called when shutdown was already requested)
- Global variables used:
  - in_restore_command (set to true)
  - shutdown_requested (checked for pending shutdown)
- Called from (representative examples):
  - RestoreArchivedFile (in xlogarchive.c)

## Notes and Other Information
- This function works in conjunction with PostRestoreCommand to bracket restore command execution
- The in_restore_command flag affects signal handler behavior, allowing immediate safe termination
- Setting this flag is crucial for proper shutdown handling during archive recovery operations
- The function ensures no shutdown requests are lost during the transition to restore command state
- This is part of PostgreSQL's archive recovery mechanism and ensures clean shutdown during restore operations
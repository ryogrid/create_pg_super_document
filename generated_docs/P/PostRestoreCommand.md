# PostRestoreCommand

## Location
src/backend/postmaster/startup.c: 282 - 287

## Overview
PostRestoreCommand is a function that cleans up the startup process state after completing a restore command execution, resetting the restore command flag to its normal state.

## Definition
```c
void PostRestoreCommand(void)
```

## Detailed Description
PostRestoreCommand serves as the cleanup counterpart to PreRestoreCommand, called after a restore command has completed execution during archive recovery. The function simply resets the global in_restore_command flag to false, returning the startup process to its normal signal handling behavior.

This function is essential for proper signal handling state management. When in_restore_command is false, the startup process returns to its standard signal handling mode where SIGTERM signals are handled according to the normal shutdown procedures rather than triggering immediate exit.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - None (only modifies global variable)
- Global variables used:
  - in_restore_command (set to false)
- Called from (representative examples):
  - [RestoreArchivedFile](../R/RestoreArchivedFile.md) (in xlogarchive.c, after restore command completion)

## Notes and Other Information
- This function works as a pair with PreRestoreCommand to bracket restore command execution
- Resetting in_restore_command is crucial for returning to normal signal handling behavior
- The function ensures that signal handlers behave correctly after restore command completion
- Part of PostgreSQL's archive recovery mechanism for proper state management
- Simple but critical for maintaining correct startup process signal handling semantics
- Must be called after every successful or failed restore command execution to maintain proper state
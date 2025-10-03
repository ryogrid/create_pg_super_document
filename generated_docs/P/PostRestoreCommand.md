# PostRestoreCommand

## Location
[src/backend/postmaster/startup.c:282-287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/startup.c#L282-L287)

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

## Simplified Source

```c
// Simplified version of PostRestoreCommand
void PostRestoreCommand(void) {
    // Reset the restore command flag to indicate completion
    in_restore_command = false;
}
```

Key simplifications made:
- No simplifications needed - function is already minimal
- Added descriptive comment explaining the purpose
- Function contains only essential logic: resetting the global flag
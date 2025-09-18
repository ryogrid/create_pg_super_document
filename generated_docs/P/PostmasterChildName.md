# PostmasterChildName

## Location
src/backend/postmaster/launch_backend.c: 214 - 230

## Overview
Returns a human-readable name string for a given PostgreSQL child process type, used for logging and identification purposes.

## Definition
```c
const char *PostmasterChildName(BackendType child_type)
```

## Detailed Description
PostmasterChildName is a simple lookup function that maps BackendType enumeration values to their corresponding descriptive names. It accesses the global `child_process_kinds` array to retrieve the name field for the specified child process type. This function is primarily used for logging, error reporting, and process identification in the PostgreSQL postmaster system.

The function provides a centralized way to get consistent naming for different types of child processes that the postmaster can spawn, including backend processes, auxiliary processes like the background writer, checkpointer, WAL writer, autovacuum processes, and others.

## Parameters / Member Variables
- `child_type`: A BackendType enumeration value specifying the type of child process (e.g., B_BACKEND, B_BG_WRITER, B_CHECKPOINTER, B_AUTOVAC_LAUNCHER, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - child_process_kinds (global array)
  - BackendType (enumeration type)
- Called from (representative examples):
  - StartChildProcess (in postmaster.c)
  - Various logging and error reporting functions

## Notes and Other Information
- The function performs a direct array lookup without bounds checking, relying on the caller to provide valid BackendType values
- The returned string is a constant and should not be modified
- The child_process_kinds array contains entries for all supported child process types, including some that cannot be launched directly (like B_WAL_SENDER)
- This function is located in src/backend/postmaster/launch_backend.c:214-217
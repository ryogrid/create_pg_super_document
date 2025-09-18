# write_nondefault_variables

## Location
src/backend/utils/misc/guc.c: 5662 - 5716

## Overview
write_nondefault_variables serializes all non-default GUC configuration variables to a binary file for sharing with exec'd backend processes.

## Definition


## Detailed Description
write_nondefault_variables is a key function in PostgreSQL's EXEC_BACKEND mechanism that writes all non-default configuration variables to a binary file. This enables newly spawned backend processes to inherit the current configuration state from the postmaster.

The function operates in the following sequence:
1. **Validation**: Ensures the context is either PGC_POSTMASTER or PGC_SIGHUP
2. **File Creation**: Opens a new temporary file (CONFIG_EXEC_PARAMS_NEW) for writing
3. **Serialization**: Iterates through guc_nondef_list and writes each non-default variable
4. **Atomic Replacement**: Renames the temporary file to the final location (CONFIG_EXEC_PARAMS)

Error handling varies by context:
- **PGC_SIGHUP context**: Logs errors at LOG level (during configuration reload)
- **PGC_POSTMASTER context**: Reports errors at ERROR level (during startup)

The function uses the global guc_nondef_list, which maintains a doubly-linked list of all GUC variables that have been set to non-default values. This optimization avoids scanning the entire configuration array.

## Parameters / Member Variables
- : GucContext indicating the calling context (PGC_POSTMASTER or PGC_SIGHUP)

## Dependencies
- Functions called/Symbols referenced:
  - AllocateFile
  - FreeFile
  - dlist_foreach
  - dlist_container
  - [write_one_nondefault_variable](write_one_nondefault_variable.md)
  - rename
  - ereport
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md)
  - [process_pm_reload_request](../p/process_pm_reload_request.md)

## Notes and Other Information
- Only available when EXEC_BACKEND is defined (Windows and some Unix configurations)
- Uses atomic file replacement (write to temp, then rename) to avoid corruption
- The function name CONFIG_EXEC_PARAMS_NEW and CONFIG_EXEC_PARAMS are defined constants for file paths
- Error reporting level depends on calling context to avoid inappropriate error escalation during reloads
- The guc_nondef_list is maintained automatically as GUC variables are modified
- File operations use PostgreSQL's AllocateFile/FreeFile for consistent error handling
- The binary format written matches what read_nondefault_variables expects
- Part of the mechanism that allows PostgreSQL to work on platforms without fork()
# shell_archive_configured

## Location
src/backend/archive/shell_archive.c: 46 - 56

## Overview
This function checks whether the shell-based WAL archiving module is properly configured by verifying that the archive_command GUC parameter has been set.

## Definition
```c
static bool shell_archive_configured(ArchiveModuleState *state)
```

## Detailed Description
The `shell_archive_configured` function serves as the configuration check callback for the shell archiving module. It determines whether the module is ready to perform archiving operations by examining the `XLogArchiveCommand` global variable, which stores the value of the `archive_command` GUC parameter.

If the archive command is properly configured (non-empty string), the function returns true. If not configured, it calls `arch_module_check_errdetail` to report the specific configuration issue and returns false.

This function is called by PostgreSQL's archiving infrastructure to validate that the archiving module is properly configured before attempting to archive WAL files.

## Parameters / Member Variables
- `state`: Pointer to ArchiveModuleState structure (currently unused by this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - XLogArchiveCommand (global variable containing archive_command GUC value)
  - arch_module_check_errdetail (error reporting function)
  - [ArchiveModuleState](../A/ArchiveModuleState.md) (parameter type)
- Called from (representative examples):
  - Referenced indirectly through shell_archive_callbacks structure

## Notes and Other Information
- This is a static function, only accessible within the shell_archive.c module
- The function is assigned to the `check_configured_cb` member of the shell_archive_callbacks structure
- The `state` parameter is currently unused but maintained for interface consistency
- The `XLogArchiveCommand` variable is defined in xlog.c and corresponds to the `archive_command` GUC parameter
- Returns true only if archive_command is set to a non-empty string
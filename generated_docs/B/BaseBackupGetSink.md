# BaseBackupGetSink

## Location
src/backend/backup/basebackup_target.c: 163 - 171

## Overview
Creates and returns a bbsink object for the specified backup target handle, delegating to the target-specific get_sink function to construct the appropriate backup sink implementation.

## Definition
```c
bbsink *BaseBackupGetSink(BaseBackupTargetHandle *handle, bbsink *next_sink)
```

## Detailed Description
This function serves as a simple wrapper that calls the target-specific get_sink function with the appropriate arguments. It passes the next_sink parameter (which may be NULL or another bbsink for chaining) and the detail_arg that was processed and stored when the handle was created. This design allows the check_detail and get_sink functions to communicate through the detail_arg, enabling flexible configuration passing between the validation and construction phases.

The function is part of the backup sink chain construction process, where multiple bbsink objects can be linked together to process backup data through various transformations or destinations.

## Parameters / Member Variables
- `handle`: BaseBackupTargetHandle containing the target type and processed detail arguments
- `next_sink`: Optional next bbsink in the processing chain (may be NULL for terminal sinks)

## Dependencies
- Functions called/Symbols referenced:
  - BaseBackupTargetHandle (struct)
  - bbsink (type)
  - Target-specific get_sink function (via handle->type->get_sink)

- Called from (representative examples):
  - SendBaseBackup

## Notes and Other Information
- Acts as a simple delegation wrapper to target-specific sink constructors
- Enables communication between check_detail and get_sink functions via detail_arg
- Supports chaining of backup sinks for multi-stage processing
- The actual bbsink implementation depends on the target type (client, server-file, etc.)
- Part of PostgreSQL's pluggable backup sink architecture
- The returned bbsink must implement the standard bbsink interface for backup data processing
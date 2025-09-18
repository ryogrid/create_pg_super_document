# server_check_detail

## Location
src/backend/backup/basebackup_target.c: 232 - 241

## Overview
Validates that a server-side backup target has a required target detail (directory path) and returns it for further processing.

## Definition
static void *server_check_detail(char *target, char *target_detail)

## Detailed Description
This function implements target-detail validation for server-side backup targets that require a target detail parameter. It ensures that users provide the necessary target detail (typically a directory path) when performing server-side backups. If no target detail is provided, the function raises a syntax error. When a valid target detail is provided, it returns the detail string for use by the backup system.

The function performs only basic presence validation - more detailed validation of the directory path and permissions checking is deferred to bbsink_server_new() where the actual backup sink is created. This separation allows for early validation of required parameters while deferring expensive operations until they're actually needed.

## Parameters / Member Variables
- `target`: The name of the backup target being validated
- `target_detail`: The detail string provided by the user (should contain directory path)

## Dependencies
- Functions called/Symbols referenced:
  - ereport (PostgreSQL error reporting)
  - errcode, errmsg (error code and message macros)
  - ERRCODE_SYNTAX_ERROR, ERROR (PostgreSQL error constants)
- Called from (representative examples):
  - BaseBackupTargetHandle (via function pointer in target type structure)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the basebackup_target.c file
- Returns the target_detail parameter unchanged when validation passes
- Raises ERROR when target_detail is NULL, which aborts the current transaction
- Used specifically for server-side backup targets that write to local directories
- Actual directory validation and permissions checking occurs later in bbsink_server_new()
- Located in src/backend/backup/basebackup_target.c at lines 232-241
- Part of PostgreSQL's two-phase validation system: early parameter presence check, then detailed validation during execution
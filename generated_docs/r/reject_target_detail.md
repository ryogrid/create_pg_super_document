# reject_target_detail

## Location
[src/backend/backup/basebackup_target.c:213-231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_target.c#L213-L231)

## Overview
Validates that a backup target does not accept target details and raises an error if any detail is provided.

## Definition
static void *reject_target_detail(char *target, char *target_detail)

## Detailed Description
This function implements target-detail validation for backup targets that do not accept additional configuration details. It serves as a validation callback in PostgreSQL's base backup system, ensuring that users don't attempt to specify target details for backup types that don't support them. When a target_detail is provided for such targets, the function raises a syntax error with an appropriate error message.

The function is designed to be used as a callback in the BaseBackupTargetType structure's check_detail field for targets that should reject any additional detail parameters.

## Parameters / Member Variables
- `target`: The name of the backup target being validated
- `target_detail`: The detail string provided by the user (should be NULL for valid calls)

## Dependencies
- Functions called/Symbols referenced:
  - ereport (PostgreSQL error reporting)
  - [errcode](../e/errcode.md), errmsg (error code and message macros)
  - ERRCODE_SYNTAX_ERROR, ERROR (PostgreSQL error constants)
- Called from (representative examples):
  - [BaseBackupTargetHandle](../B/BaseBackupTargetHandle.md) (via function pointer in target type structure)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the basebackup_target.c file
- Returns NULL when target_detail is NULL (valid case)
- Raises ERROR when target_detail is not NULL, which aborts the current transaction
- Used for backup targets like 'blackhole' that don't require or accept additional configuration
- Located in src/backend/backup/basebackup_target.c at lines 213-231
- Part of PostgreSQL's input validation system for base backup commands
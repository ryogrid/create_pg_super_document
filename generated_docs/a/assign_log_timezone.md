# assign_log_timezone

## Location
src/backend/commands/variable.c: 454 - 462

## Overview
This is a GUC assign hook function that sets the log_timezone global variable when the log_timezone configuration parameter is changed in PostgreSQL.

## Definition


## Detailed Description
The  function serves as the assignment hook for PostgreSQL's  configuration parameter. It is called after the  function has successfully validated the new timezone value. The function simply assigns the pre-validated timezone structure (stored in the  parameter) to the global  variable.

This function is part of the GUC (Grand Unified Configuration) system's hook mechanism, which allows for custom validation and assignment logic for configuration parameters.

## Parameters / Member Variables
- : The new timezone string value (not used in this function)
- : Pointer to the pre-validated pg_tz structure from the check hook

## Dependencies
- Functions called/Symbols referenced:
  - [pg_tz](../p/pg_tz.md) (type reference)
- Called from (representative examples):
  - PostgreSQL GUC system (via function pointer in GUC_HOOKS_H)

## Notes and Other Information
- This function is always called after successful validation by check_log_timezone
- The actual timezone object is passed via the extra parameter, not derived from newval
- The function directly assigns to the global log_timezone variable
- Part of the standard GUC assign hook pattern in PostgreSQL
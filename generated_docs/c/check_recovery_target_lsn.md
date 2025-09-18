# check_recovery_target_lsn

## Location
src/backend/access/transam/xlogrecovery.c: 4812 - 4834

## Overview
A GUC (Grand Unified Configuration) check hook function that validates the  parameter, ensuring it contains a properly formatted LSN (Log Sequence Number) value before the configuration setting is accepted.

## Definition
```c
bool check_recovery_target_lsn(char **newval, void **extra, GucSource source)
```

## Detailed Description
This function serves as a validation hook for the  PostgreSQL configuration parameter. When a user attempts to set this parameter, PostgreSQL calls this function to verify that the provided value is valid. The function parses the input string as an LSN using the internal LSN parsing function and stores the parsed LSN value in the extra data structure for later use by the assign hook. If the input string is empty, it's considered valid (allowing the parameter to be unset). If the input contains an invalid LSN format, the function returns false to reject the configuration change.

## Parameters / Member Variables
- : Pointer to the new string value being assigned to the GUC parameter
- : Pointer to store additional data (parsed LSN) that will be passed to the assign hook
- : The source of the GUC setting (e.g., configuration file, command line, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - pg_lsn_in_internal (parses LSN string to XLogRecPtr)
  - guc_malloc (allocates memory for storing parsed LSN)
  - GucSource (enum type for configuration source)
- Called from (representative examples):
  - PostgreSQL GUC system when recovery_target_lsn parameter is being set

## Notes and Other Information
- This is part of PostgreSQL's point-in-time recovery (PITR) system
- The parsed LSN is stored in dynamically allocated memory and passed to the corresponding assign hook
- Empty strings are accepted, allowing the recovery target to be unset
- The function follows the standard GUC check hook pattern, returning true for valid values and false for invalid ones
- The allocated extra data must be freed by the corresponding assign hook or show hook
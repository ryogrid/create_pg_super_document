# call_bool_check_hook

## Location
src/backend/utils/misc/guc.c: 6812 - 6845

## Overview
Executes the validation check hook for boolean GUC parameters, managing error reporting protocol and returning validation results.

## Definition

```c
static bool
call_bool_check_hook(struct config_bool *conf, bool *newval, void **extra,
					 GucSource source, int elevel)
```
## Detailed Description
call_bool_check_hook is a convenience function that standardizes the process of invoking validation check hooks for boolean GUC parameters. The function handles the complete protocol for check hook execution, including error state management and comprehensive error reporting when validation fails.

Before calling the hook, it resets all error reporting variables to ensure clean state. If the hook fails, it constructs detailed error messages using information provided by the hook or falls back to default error messages. The function supports custom error codes, messages, details, and hints as set by the check hook through the GUC_check_* mechanism.

## Parameters / Member Variables
- : Pointer to the config_bool structure containing the boolean GUC configuration
- : Pointer to the proposed new boolean value to be validated
- : Pointer to pointer for hook-specific extra data storage
- : GucSource indicating the origin of the configuration change
- : Error level for reporting validation failures (ERROR, WARNING, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - config_bool
  - GucSource
  - ereport
  - errcode
  - errmsg_internal
  - errmsg
  - errdetail_internal
  - errhint
  - FlushErrorState
- Called from (representative examples):
  - InitializeOneGUCOption
  - parse_and_validate_value
  - newval

## Notes and Other Information
- Returns true if validation succeeds or no check hook is defined, false if validation fails
- Resets GUC error reporting variables (errcode_value, errmsg_string, errdetail_string, errhint_string) before hook execution
- Provides detailed error messages using hook-supplied information when available
- Falls back to standard "invalid value for parameter" message if hook doesn't provide custom message
- Uses FlushErrorState() to clean up any temporary strings created during error reporting
- Part of PostgreSQL's GUC validation infrastructure specifically for boolean parameters
- Maintains consistent error reporting protocol across all boolean GUC parameter validations
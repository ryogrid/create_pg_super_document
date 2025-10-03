# call_bool_check_hook

## Location
[src/backend/utils/misc/guc.c:6812-6845](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L6812-L6845)

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
- `*conf`: Pointer to the config_bool structure containing the boolean GUC configuration
- `*newval`: Pointer to the proposed new boolean value to be validated
- `**extra`: Pointer to pointer for hook-specific extra data storage
- `source`: GucSource indicating the origin of the configuration change
- `elevel`: Error level for reporting validation failures (ERROR, WARNING, etc.)
## Dependencies
- Functions called/Symbols referenced:
  - config_bool
  - GucSource
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg_internal](../e/errmsg_internal.md)
  - [errmsg](../e/errmsg.md)
  - [errdetail_internal](../e/errdetail_internal.md)
  - [errhint](../e/errhint.md)
  - [FlushErrorState](../F/FlushErrorState.md)
- Called from (representative examples):
  - [InitializeOneGUCOption](../I/InitializeOneGUCOption.md)
  - [parse_and_validate_value](../p/parse_and_validate_value.md)
  - newval

## Notes and Other Information
- Returns true if validation succeeds or no check hook is defined, false if validation fails
- Resets GUC error reporting variables (errcode_value, errmsg_string, errdetail_string, errhint_string) before hook execution
- Provides detailed error messages using hook-supplied information when available
- Falls back to standard "invalid value for parameter" message if hook doesn't provide custom message
- Uses FlushErrorState() to clean up any temporary strings created during error reporting
- Part of PostgreSQL's GUC validation infrastructure specifically for boolean parameters
- Maintains consistent error reporting protocol across all boolean GUC parameter validations

## Simplified Source

```c
// Simplified version of call_bool_check_hook
static bool call_bool_check_hook(struct config_bool *conf, bool *newval, void **extra,
                                GucSource source, int elevel) {
    // Quick exit if no validation hook is defined
    if (!conf->check_hook)
        return true;

    // Reset error reporting variables to clean state
    GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
    GUC_check_errmsg_string = NULL;
    GUC_check_errdetail_string = NULL;
    GUC_check_errhint_string = NULL;

    // Call the validation hook
    if (!conf->check_hook(newval, extra, source)) {
        // Hook failed - report comprehensive error
        ereport(elevel,
                (errcode(GUC_check_errcode_value),
                 GUC_check_errmsg_string ?
                 errmsg_internal("%s", GUC_check_errmsg_string) :
                 errmsg("invalid value for parameter \"%s\": %d",
                        conf->gen.name, (int) *newval),
                 GUC_check_errdetail_string ?
                 errdetail_internal("%s", GUC_check_errdetail_string) : 0,
                 GUC_check_errhint_string ?
                 errhint("%s", GUC_check_errhint_string) : 0));

        // Clean up any temporary error strings
        FlushErrorState();
        return false;
    }

    return true;
}
```

Key simplifications made:
- Preserved the essential validation workflow and error reporting logic
- Maintained the complete ereport call structure as it's critical for proper error handling
- Kept all error state management as it's fundamental to the GUC system
- Added clearer comments explaining each major step
- Reformatted for better readability while preserving all functionality
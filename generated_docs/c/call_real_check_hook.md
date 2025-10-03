# call_real_check_hook

## Location
[src/backend/utils/misc/guc.c:6880-6913](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L6880-L6913)

## Overview
A static function that validates real (floating-point) GUC parameters by calling their associated check hooks and handling validation errors with proper error reporting.

## Definition

```c
static bool
call_real_check_hook(struct config_real *conf, double *newval, void **extra,
					 GucSource source, int elevel)
```
## Detailed Description
This function serves as a validation wrapper for real (double/floating-point) type GUC parameters in PostgreSQL's configuration system. It follows the same pattern as call_int_check_hook but handles double values instead of integers. When a GUC parameter has an associated check hook function, this function calls that hook to validate the proposed new value.

The function performs the same validation workflow: checking for hook existence, resetting global error variables, calling the validation hook, and reporting detailed errors on validation failure. The error message format uses '%g' for floating-point value display instead of '%d' used for integers.

## Parameters / Member Variables
- `*conf`: Pointer to the config_real structure containing the GUC parameter configuration and its check hook
- `*newval`: Pointer to the double value being validated
- `**extra`: Pointer to extra data that may be set by the check hook for use during assignment
- `source`: The source of the configuration change (e.g., configuration file, command line, etc.)
- `elevel`: Error level for reporting validation failures (e.g., ERROR, WARNING)
## Dependencies
- Functions called/Symbols referenced:
  - [config_real](config_real.md) (struct type)
  - GucSource (enum type)
  - [errdetail_internal](../e/errdetail_internal.md)
  - [FlushErrorState](../F/FlushErrorState.md)
  - ereport (via error reporting macros)
  - [errcode](../e/errcode.md), errmsg_internal, errmsg, errhint (error reporting functions)

- Called from (representative examples):
  - [InitializeOneGUCOption](../I/InitializeOneGUCOption.md)
  - [parse_and_validate_value](../p/parse_and_validate_value.md)

## Notes and Other Information
- This is a static function internal to the GUC system implementation
- Parallel implementation to call_int_check_hook but for floating-point values
- Uses '%g' format specifier for displaying double values in error messages
- Part of PostgreSQL's type-specific validation framework for GUC parameters
- Maintains the same global error variable coordination pattern as other check hook callers
- Returns true for successful validation, false for validation failure

## Simplified Source

```c
// Simplified version of call_real_check_hook
static bool call_real_check_hook(struct config_real *conf, double *newval, void **extra,
                                GucSource source, int elevel) {
    // Quick success if no validation hook is defined
    if (!conf->check_hook)
        return true;

    // Reset global error variables before validation
    GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
    GUC_check_errmsg_string = NULL;
    GUC_check_errdetail_string = NULL;
    GUC_check_errhint_string = NULL;

    // Call the validation hook for the new value
    if (!conf->check_hook(newval, extra, source)) {
        // Report validation failure with appropriate error details
        ereport(elevel,
                (errcode(GUC_check_errcode_value),
                 GUC_check_errmsg_string ?
                 errmsg_internal("%s", GUC_check_errmsg_string) :
                 errmsg("invalid value for parameter \"%s\": %g",
                        conf->gen.name, *newval),
                 // Add optional error detail and hint if available
                 GUC_check_errdetail_string ?
                 errdetail_internal("%s", GUC_check_errdetail_string) : 0,
                 GUC_check_errhint_string ?
                 errhint("%s", GUC_check_errhint_string) : 0));

        // Clean up any error context strings
        FlushErrorState();
        return false;
    }

    return true;
}
```

Key simplifications made:
- Added descriptive comments for each major logic block
- Preserved the exact validation workflow and error handling
- Maintained the complex error reporting structure as it's essential
- Focused on the main execution path while keeping all error handling intact
- No significant code reduction as error handling is critical for this function
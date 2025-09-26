# call_real_check_hook

## Location
src/backend/utils/misc/guc.c: 6880 - 6913

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
- : Pointer to the config_real structure containing the GUC parameter configuration and its check hook
- : Pointer to the double value being validated
- : Pointer to extra data that may be set by the check hook for use during assignment
- : The source of the configuration change (e.g., configuration file, command line, etc.)
- : Error level for reporting validation failures (e.g., ERROR, WARNING)

## Dependencies
- Functions called/Symbols referenced:
  - config_real (struct type)
  - GucSource (enum type)
  - errdetail_internal
  - FlushErrorState
  - ereport (via error reporting macros)
  - errcode, errmsg_internal, errmsg, errhint (error reporting functions)

- Called from (representative examples):
  - InitializeOneGUCOption
  - parse_and_validate_value

## Notes and Other Information
- This is a static function internal to the GUC system implementation
- Parallel implementation to call_int_check_hook but for floating-point values
- Uses '%g' format specifier for displaying double values in error messages
- Part of PostgreSQL's type-specific validation framework for GUC parameters
- Maintains the same global error variable coordination pattern as other check hook callers
- Returns true for successful validation, false for validation failure
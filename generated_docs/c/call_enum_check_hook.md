# call_enum_check_hook

## Location
[src/backend/utils/misc/guc.c:6964-6996](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L6964-L6996)

## Overview
A static function that validates enumeration GUC parameters by calling their associated check hooks and providing enum-specific error reporting with value name lookup.

## Definition

```c
static bool
call_enum_check_hook(struct config_enum *conf, int *newval, void **extra,
					 GucSource source, int elevel)
```
## Detailed Description
This function validates enumeration-type GUC parameters following the same basic pattern as other type-specific check hook callers. The key distinction is in error reporting: when validation fails, it uses config_enum_lookup_by_value() to convert the integer enum value back to its string representation for display in error messages.

Enum GUC parameters internally store their values as integers (typically corresponding to array indices or enum constants), but users interact with them using string names. This function bridges that gap by providing meaningful error messages that show the enum name rather than its internal integer value.

The validation workflow mirrors other check hook callers: check for hook existence, reset global error variables, call the validation hook, and report detailed errors on failure. The enum-specific enhancement is the value-to-name lookup for user-friendly error reporting.

## Parameters / Member Variables
- `*conf`: Pointer to the config_enum structure containing the GUC parameter configuration and its check hook
- `*newval`: Pointer to the integer enum value being validated (internal representation)
- `**extra`: Pointer to extra data that may be set by the check hook for use during assignment
- `source`: The source of the configuration change (e.g., configuration file, command line, etc.)
- `elevel`: Error level for reporting validation failures (e.g., ERROR, WARNING)
## Dependencies
- Functions called/Symbols referenced:
  - [config_enum](config_enum.md) (struct type)
  - GucSource (enum type)
  - [config_enum_lookup_by_value](config_enum_lookup_by_value.md) (for error message display)
  - [errdetail_internal](../e/errdetail_internal.md)
  - [FlushErrorState](../F/FlushErrorState.md)
  - ereport (via error reporting macros)
  - [errcode](../e/errcode.md), errmsg_internal, errmsg, errhint (error reporting functions)

- Called from (representative examples):
  - [InitializeOneGUCOption](../I/InitializeOneGUCOption.md)
  - [parse_and_validate_value](../p/parse_and_validate_value.md)

## Notes and Other Information
- Provides user-friendly error messages by converting integer values to enum names
- Uses config_enum_lookup_by_value() to translate internal integer values to displayable strings
- Part of PostgreSQL's enumeration support in the GUC configuration system
- Follows the same global error variable coordination pattern as other check hook callers
- Essential for GUC parameters that have a limited set of valid named options (like log levels, synchronous commit modes, etc.)
- Returns true for successful validation, false for validation failure

## Simplified Source

```c
// Simplified version of call_enum_check_hook
static bool call_enum_check_hook(struct config_enum *conf, int *newval, void **extra,
                                 GucSource source, int elevel) {
    // Quick exit if no validation hook is configured
    if (!conf->check_hook)
        return true;

    // Reset global error message variables
    GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
    GUC_check_errmsg_string = NULL;
    GUC_check_errdetail_string = NULL;
    GUC_check_errhint_string = NULL;

    // Call the validation hook
    if (!conf->check_hook(newval, extra, source)) {
        // Report validation failure with enum-specific error message
        ereport(elevel,
                (errcode(GUC_check_errcode_value),
                 GUC_check_errmsg_string ?
                 errmsg_internal("%s", GUC_check_errmsg_string) :
                 errmsg("invalid value for parameter \"%s\": \"%s\"",
                        conf->gen.name,
                        config_enum_lookup_by_value(conf, *newval)),
                 // Add optional error detail and hint if available
                 GUC_check_errdetail_string ?
                 errdetail_internal("%s", GUC_check_errdetail_string) : 0,
                 GUC_check_errhint_string ?
                 errhint("%s", GUC_check_errhint_string) : 0));

        // Clean up error context
        FlushErrorState();
        return false;
    }

    return true;
}
```

Key simplifications made:
- Consolidated the error reporting structure for clarity
- Added descriptive comments for each major step
- Preserved the enum-specific value lookup logic
- Maintained the global error variable coordination pattern
- Focused on the main validation workflow
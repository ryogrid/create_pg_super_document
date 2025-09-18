# DefineCustomIntVariable

## Location
src/backend/utils/misc/guc.c: 5166 - 5195

## Overview
Public API function that allows PostgreSQL extensions to define custom integer GUC variables with range validation and callback hooks.

## Definition
```c
void
DefineCustomIntVariable(const char *name,
                        const char *short_desc,
                        const char *long_desc,
                        int *valueAddr,
                        int bootValue,
                        int minValue,
                        int maxValue,
                        GucContext context,
                        int flags,
                        GucIntCheckHook check_hook,
                        GucIntAssignHook assign_hook,
                        GucShowHook show_hook)
```

## Detailed Description
This function provides the public interface for PostgreSQL extensions to create custom integer-typed GUC configuration variables. It allocates and initializes a config_int structure, sets up the integer-specific fields including variable pointer, default value, and valid range constraints, then integrates the variable into the GUC system. The function supports range validation through min/max values and optional validation and callback hooks for advanced customization.

## Parameters / Member Variables
- `name`: The name of the custom GUC variable (must be unique)
- `short_desc`: Brief description shown in pg_settings and help text
- `long_desc`: Detailed description (can be NULL)
- `valueAddr`: Pointer to the integer variable that stores the current value
- `bootValue`: Initial/default value for the variable
- `minValue`: Minimum allowed value for the variable
- `maxValue`: Maximum allowed value for the variable
- `context`: GUC context determining who can modify the variable
- `flags`: Bitfield of GUC flags controlling variable behavior
- `check_hook`: Optional validation function called before value changes
- `assign_hook`: Optional callback called after successful value assignment
- `show_hook`: Optional function to customize how the value is displayed

## Dependencies
- Functions called/Symbols referenced:
  - init_custom_variable
  - define_custom_variable
  - PGC_INT (config type constant)
  - config_int (structure type)
- Called from (representative examples):
  - _PG_init functions in test modules
  - delay_execution module initialization
  - worker_spi module initialization

## Notes and Other Information
- This is a public function exported in the PostgreSQL API for extensions
- The valueAddr parameter must point to a variable with sufficient lifetime (typically static/global)
- Range validation is automatically performed by the GUC system using min/max values
- The bootValue is used as both the initial value and reset value and must be within [minValue, maxValue]
- Hook functions are optional and can be NULL
- Variable names should follow the pattern 'extension_name.variable_name' to avoid conflicts
- All custom variables are grouped under CUSTOM_OPTIONS in pg_settings
- The function performs security checks inherited from init_custom_variable
- Memory for the config structure is allocated via guc_malloc and managed by the GUC system
- Range violations are automatically caught and reported as errors by the GUC infrastructure
# DefineCustomBoolVariable

## Location
src/backend/utils/misc/guc.c: 5140 - 5165

## Overview
Public API function that allows PostgreSQL extensions to define custom boolean GUC variables with validation and callback hooks.

## Definition
```c
void
DefineCustomBoolVariable(const char *name,
                         const char *short_desc,
                         const char *long_desc,
                         bool *valueAddr,
                         bool bootValue,
                         GucContext context,
                         int flags,
                         GucBoolCheckHook check_hook,
                         GucBoolAssignHook assign_hook,
                         GucShowHook show_hook)
```

## Detailed Description
This function provides the public interface for PostgreSQL extensions to create custom boolean-typed GUC configuration variables. It allocates and initializes a config_bool structure, sets up the boolean-specific fields including the variable pointer and default value, and integrates the variable into the GUC system. The function supports optional validation and callback hooks that are invoked when the variable is checked, assigned, or displayed.

## Parameters / Member Variables
- `name`: The name of the custom GUC variable (must be unique)
- `short_desc`: Brief description shown in pg_settings and help text
- `long_desc`: Detailed description (can be NULL)
- `valueAddr`: Pointer to the boolean variable that stores the current value
- `bootValue`: Initial/default value for the variable
- `context`: GUC context determining who can modify the variable
- `flags`: Bitfield of GUC flags controlling variable behavior
- `check_hook`: Optional validation function called before value changes
- `assign_hook`: Optional callback called after successful value assignment  
- `show_hook`: Optional function to customize how the value is displayed

## Dependencies
- Functions called/Symbols referenced:
  - [init_custom_variable](../i/init_custom_variable.md)
  - [define_custom_variable](../d/define_custom_variable.md)
  - PGC_BOOL (config type constant)
  - config_bool (structure type)
- Called from (representative examples):
  - [_PG_init](../P/_PG_init.md) functions in various extensions
  - plperl module initialization
  - test_oat_hooks module initialization

## Notes and Other Information
- This is a public function exported in the PostgreSQL API for extensions
- The valueAddr parameter must point to a variable with sufficient lifetime (typically static/global)
- The bootValue is used as both the initial value and reset value
- Hook functions are optional and can be NULL
- [Variable](../V/Variable.md) names should follow the pattern 'extension_name.variable_name' to avoid conflicts
- All custom variables are grouped under CUSTOM_OPTIONS in pg_settings
- The function performs security checks inherited from init_custom_variable
- Memory for the config structure is allocated via guc_malloc and managed by the GUC system
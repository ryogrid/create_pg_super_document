# DefineCustomRealVariable

## Location
[src/backend/utils/misc/guc.c:5196-5225](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L5196-L5225)

## Overview
Registers a custom floating-point configuration variable in PostgreSQL's Grand Unified Configuration (GUC) system, allowing extensions to define their own real-valued parameters that can be set and managed like built-in configuration options.

## Definition
```c
void DefineCustomRealVariable(const char *name,
                              const char *short_desc,
                              const char *long_desc,
                              double *valueAddr,
                              double bootValue,
                              double minValue,
                              double maxValue,
                              GucContext context,
                              int flags,
                              GucRealCheckHook check_hook,
                              GucRealAssignHook assign_hook,
                              GucShowHook show_hook)
```

## Detailed Description
This function is part of PostgreSQL's extensible configuration system, allowing extensions and custom code to define floating-point configuration variables that integrate seamlessly with the GUC infrastructure. The function creates a config_real structure and registers it with the GUC system, enabling the variable to be set through postgresql.conf, ALTER SYSTEM, SET commands, and other standard configuration mechanisms.

The function initializes all necessary metadata for the real variable including its bounds, hooks for validation and assignment, and display formatting. Once registered, the variable becomes part of the global configuration state and can be queried and modified using standard PostgreSQL configuration interfaces.

## Parameters / Member Variables
- `name`: The name of the configuration variable (must be unique)
- `short_desc`: Brief description shown in pg_settings view
- `long_desc`: Detailed description for documentation
- `valueAddr`: Pointer to the double variable that will hold the current value
- `bootValue`: Initial/default value assigned at startup
- `minValue`: Minimum allowed value for the variable
- `maxValue`: Maximum allowed value for the variable
- `context`: GucContext specifying when the variable can be changed (e.g., PGC_SIGHUP, PGC_USERSET)
- `flags`: Bitwise flags controlling variable behavior and display
- `check_hook`: Optional validation function called before value changes
- `assign_hook`: Optional function called after successful value assignment
- `show_hook`: Optional function to customize how the value is displayed

## Dependencies
- Functions called/Symbols referenced:
  - [init_custom_variable](../i/init_custom_variable.md)
  - [define_custom_variable](../d/define_custom_variable.md)
  - PGC_REAL
  - [config_real](../c/config_real.md)
  - GucContext
- Called from (representative examples):
  - Extension initialization functions
  - Module load callbacks

## Notes and Other Information
The variable remains registered for the lifetime of the process. The valueAddr pointer must remain valid throughout the process lifetime as the GUC system will read from and write to this location. The boot value is used both as the initial value and as the reset value when the configuration is reset to defaults. Bounds checking is automatically enforced by the GUC system using the provided min and max values.
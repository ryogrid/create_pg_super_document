# DefineCustomEnumVariable

## Location
src/backend/utils/misc/guc.c: 5251 - 5286

## Overview
Registers a custom enumeration configuration variable in PostgreSQL's Grand Unified Configuration (GUC) system, allowing extensions to define their own enum-valued parameters with predefined options that can be set and managed like built-in configuration options.

## Definition
```c
void DefineCustomEnumVariable(const char *name,
                              const char *short_desc,
                              const char *long_desc,
                              int *valueAddr,
                              int bootValue,
                              const struct config_enum_entry *options,
                              GucContext context,
                              int flags,
                              GucEnumCheckHook check_hook,
                              GucEnumAssignHook assign_hook,
                              GucShowHook show_hook)
```

## Detailed Description
This function is part of PostgreSQL's extensible configuration system, allowing extensions and custom code to define enumeration configuration variables that integrate seamlessly with the GUC infrastructure. The function creates a config_enum structure and registers it with the GUC system, enabling the variable to be set through postgresql.conf, ALTER SYSTEM, SET commands, and other standard configuration mechanisms.

Enum variables provide a controlled set of valid string values that map to integer constants, offering both user-friendly string names and efficient integer storage. The GUC system automatically handles validation against the provided options array and conversion between string and integer representations.

## Parameters / Member Variables
- `name`: The name of the configuration variable (must be unique)
- `short_desc`: Brief description shown in pg_settings view
- `long_desc`: Detailed description for documentation
- `valueAddr`: Pointer to the int variable that will hold the current enum value
- `bootValue`: Initial/default integer value assigned at startup
- `options`: Array of config_enum_entry structures defining valid string/int pairs
- `context`: GucContext specifying when the variable can be changed (e.g., PGC_SIGHUP, PGC_USERSET)
- `flags`: Bitwise flags controlling variable behavior and display
- `check_hook`: Optional validation function called before value changes
- `assign_hook`: Optional function called after successful value assignment
- `show_hook`: Optional function to customize how the value is displayed

## Dependencies
- Functions called/Symbols referenced:
  - init_custom_variable
  - define_custom_variable
  - PGC_ENUM
  - config_enum
  - config_enum_entry
  - GucContext
- Called from (representative examples):
  - Extension initialization functions
  - Module load callbacks

## Notes and Other Information
The variable remains registered for the lifetime of the process. The valueAddr pointer and options array must remain valid throughout the process lifetime as the GUC system will reference them continuously. The options array must be NULL-terminated with a {NULL, 0, false} entry. The boot value is used both as the initial value and as the reset value when the configuration is reset to defaults. String-to-integer mapping is automatically handled by the GUC system using the provided options array.
# config_string

## Location
src/include/utils/guc_tables.h: 254 - 267

## Overview
The `config_string` struct represents a string-valued configuration parameter in PostgreSQL's GUC system, managing runtime parameters that store text values with support for NULL values and custom validation hooks.

## Definition
```c
struct config_string
{
    struct config_generic gen;
    /* constant fields, must be set correctly in initial value: */
    char      **variable;
    const char *boot_val;
    GucStringCheckHook check_hook;
    GucStringAssignHook assign_hook;
    GucShowHook show_hook;
    /* variable fields, initialized at runtime: */
    char       *reset_val;
    void       *reset_extra;
};
```

## Detailed Description
The `config_string` structure handles string-valued configuration parameters in PostgreSQL's GUC system. It provides special handling for NULL values, where a NULL boot_val is allowed and leads to both reset_val and the actual variable being NULL. However, NULL values cannot be set through normal GUC APIs after initialization, and display operations show NULL as empty strings. This structure manages parameters like `log_destination`, `shared_preload_libraries`, and database connection strings that require text storage and validation.

## Parameters / Member Variables
- `gen`: Base `config_generic` structure containing common configuration metadata
- `variable`: Double pointer to the actual string variable storing the current parameter value
- `boot_val`: Default string value used during initialization (can be NULL)
- `check_hook`: Optional function pointer for custom string validation beyond basic syntax
- `assign_hook`: Optional function pointer called when the parameter value is assigned
- `show_hook`: Optional function pointer for custom formatting when displaying the parameter
- `reset_val`: String value stored for transaction rollback and RESET command operations
- `reset_extra`: Additional context data for reset operations, managed by hooks

## Dependencies
- Functions called/Symbols referenced:
  - config_generic
- Called from (representative examples):
  - string_field_used
  - set_string_field
  - build_guc_variables
  - SelectConfigFiles
  - DefineCustomStringVariable
  - call_string_check_hook

## Notes and Other Information
- Special NULL handling: boot_val can be NULL, but NULL cannot be set via normal GUC APIs afterward
- NULL values are displayed as empty strings in SHOW commands and similar operations
- Memory management is critical as strings are dynamically allocated and must be properly freed
- Callers using NULL boot_val should override the setting during startup or ensure NULL semantics match empty string behavior
- Supports transaction-safe parameter changes through reset mechanisms
- Used for configuration parameters requiring text storage such as file paths, connection strings, and comma-separated lists
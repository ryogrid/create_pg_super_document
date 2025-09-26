# config_enum

## Location
[src/include/utils/guc_tables.h:268-323](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/guc_tables.h#L268-L323)

## Overview
The `config_enum` struct represents an enumerated configuration parameter in PostgreSQL's GUC system, managing runtime parameters that accept predefined string values mapped to integer constants.

## Definition
```c
struct config_enum
{
    struct config_generic gen;
    /* constant fields, must be set correctly in initial value: */
    int            *variable;
    int             boot_val;
    const struct config_enum_entry *options;
    GucEnumCheckHook check_hook;
    GucEnumAssignHook assign_hook;
    GucShowHook show_hook;
    /* variable fields, initialized at runtime: */
    int             reset_val;
    void           *reset_extra;
};
```

## Detailed Description
The `config_enum` structure handles enumerated configuration parameters in PostgreSQL's GUC system, where string values from a predefined set are mapped to integer constants. This provides type-safe configuration with user-friendly string names while maintaining efficient integer storage internally. The structure uses a `config_enum_entry` array to define the valid name-value pairs, supporting hidden options that are accepted but not displayed in help output. Examples include parameters like `log_statement` (none/ddl/mod/all) and `wal_level` (minimal/replica/logical).

## Parameters / Member Variables
- `gen`: Base `config_generic` structure containing common configuration metadata
- `variable`: Pointer to the integer variable storing the current enumerated value
- `boot_val`: Default integer value used during database initialization
- `options`: Array of `config_enum_entry` structures defining valid string-to-integer mappings
- `check_hook`: Optional function pointer for custom validation logic beyond enum value checking
- `assign_hook`: Optional function pointer called when the parameter value changes
- `show_hook`: Optional function pointer for custom display formatting of the parameter value
- `reset_val`: Integer value stored for transaction rollback and RESET command support
- `reset_extra`: Additional context data for reset operations, managed by hooks

## Dependencies
- Functions called/Symbols referenced:
  - config_generic
  - config_enum_entry
  - config_bool
  - config_int
  - config_real
  - config_string
- Called from (representative examples):
  - config_enum_lookup_by_value
  - config_enum_lookup_by_name
  - config_enum_get_options
  - DefineCustomEnumVariable
  - call_enum_check_hook

## Notes and Other Information
- Provides type-safe enumerated values with user-friendly string names and efficient integer storage
- The `config_enum_entry` array must be NULL-terminated and defines name-value-hidden triplets
- Hidden enum values are accepted for backward compatibility but not shown in help output
- Supports bidirectional conversion between string names and integer values through lookup functions
- Used extensively for parameters with limited valid options like logging levels, WAL modes, and operational states
- Validation automatically ensures only defined enum values are accepted
- Transaction-safe through reset_val mechanism like other GUC parameter types
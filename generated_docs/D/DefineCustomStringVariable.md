# DefineCustomStringVariable

## Location
[src/backend/utils/misc/guc.c:5226-5250](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L5226-L5250)

## Overview
Registers a custom string configuration variable in PostgreSQL's Grand Unified Configuration (GUC) system, allowing extensions to define their own string-valued parameters that can be set and managed like built-in configuration options.

## Definition
```c
void DefineCustomStringVariable(const char *name,
                                const char *short_desc,
                                const char *long_desc,
                                char **valueAddr,
                                const char *bootValue,
                                GucContext context,
                                int flags,
                                GucStringCheckHook check_hook,
                                GucStringAssignHook assign_hook,
                                GucShowHook show_hook)
```

## Detailed Description
This function is part of PostgreSQL's extensible configuration system, allowing extensions and custom code to define string configuration variables that integrate seamlessly with the GUC infrastructure. The function creates a config_string structure and registers it with the GUC system, enabling the variable to be set through postgresql.conf, ALTER SYSTEM, SET commands, and other standard configuration mechanisms.

The function initializes all necessary metadata for the string variable including hooks for validation, assignment, and display formatting. String variables automatically handle memory management for their values, with the GUC system managing allocation and deallocation as values change.

## Parameters / Member Variables
- `name`: The name of the configuration variable (must be unique)
- `short_desc`: Brief description shown in pg_settings view
- `long_desc`: Detailed description for documentation
- `valueAddr`: Pointer to a char* that will hold the current string value
- `bootValue`: Initial/default string value assigned at startup
- `context`: GucContext specifying when the variable can be changed (e.g., PGC_SIGHUP, PGC_USERSET)
- `flags`: Bitwise flags controlling variable behavior and display
- `check_hook`: Optional validation function called before value changes
- `assign_hook`: Optional function called after successful value assignment
- `show_hook`: Optional function to customize how the value is displayed

## Dependencies
- Functions called/Symbols referenced:
  - [init_custom_variable](../i/init_custom_variable.md)
  - [define_custom_variable](../d/define_custom_variable.md)
  - PGC_STRING
  - config_string
  - GucContext
- Called from (representative examples):
  - [_PG_init](../P/_PG_init.md) (plperl.c:418, plperl.c:440, plperl.c:448)
  - [_PG_init](../P/_PG_init.md) (pltcl.c:465, pltcl.c:472)
  - [_PG_init](../P/_PG_init.md) (ssl_passphrase_func.c:38)
  - [_PG_init](../P/_PG_init.md) (worker_spi.c:329, worker_spi.c:338)

## Notes and Other Information
The variable remains registered for the lifetime of the process. The valueAddr pointer must remain valid throughout the process lifetime, and the GUC system will automatically manage the string memory pointed to by *valueAddr. The boot value is used both as the initial value and as the reset value when the configuration is reset to defaults. String values are automatically copied and managed by the GUC system, so the bootValue parameter can be a static string or temporary value.
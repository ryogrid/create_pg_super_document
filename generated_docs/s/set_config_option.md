# set_config_option

## Location
[src/backend/utils/misc/guc.c:3345-3384](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L3345-L3384)

## Overview
Primary external interface for setting PostgreSQL configuration parameters with comprehensive access control, validation, and context management.

## Definition

```c
int
set_config_option(const char *name, const char *value,
				  GucContext context, GucSource source,
				  GucAction action, bool changeVal, int elevel,
				  bool is_reload)
```
## Detailed Description
This function serves as the main entry point for setting configuration parameters in PostgreSQL. It handles privilege checking based on the source of the configuration change and delegates the actual work to set_config_with_handle().

The function implements a security model where interactive sources (like SQL commands) are subject to normal user privilege checks, while non-interactive sources (like config files, defaults, and pg_db_role_setting entries) are treated as having administrative privileges. The key distinction is made between PGC_S_INTERACTIVE and higher privilege sources.

The function supports multiple operational modes through its parameters: it can validate without changing (changeVal=false), apply changes at different transaction scopes (via action parameter), and handle reloading of existing settings (is_reload=true).

Return values indicate the outcome: +1 for successful application, 0 for validation errors (when elevel < ERROR), and -1 for cases where validation passed but the value wasn't applied due to operational constraints.

## Parameters / Member Variables
- : The configuration parameter name to set
- : The new value as a string (NULL means set to default value)
- : The GUC context level (e.g., PGC_SUSET, PGC_USERSET) determining access requirements
- : Source of the configuration change (e.g., PGC_S_FILE, PGC_S_USER, PGC_S_INTERACTIVE)
- : Whether to set globally, locally to current transaction, or just for function duration
- : If false, perform validation only without actually changing the value
- : Error reporting level to use, or 0 for automatic choice
- : True when loading settings from another process (affects error handling)

## Dependencies
- Functions called/Symbols referenced:
  - GucContext, GucSource, GucAction (enum types)
  - [GetUserId](../G/GetUserId.md) (current user identification)
  - [set_config_with_handle](set_config_with_handle.md) (actual implementation)
  - BOOTSTRAP_SUPERUSERID (superuser constant)
  - PGC_S_INTERACTIVE, PGC_S_CLIENT (source constants)
- Called from (representative examples):
  - [SetConfigOption](../S/SetConfigOption.md) (public wrapper)
  - [ExecSetVariableStmt](../E/ExecSetVariableStmt.md) (SQL SET command handling)
  - [ProcessGUCArray](../P/ProcessGUCArray.md) (array parameter processing)
  - [set_config_by_name](set_config_by_name.md) (function-based interface)
  - [RestrictSearchPath](../R/RestrictSearchPath.md) (search path manipulation)

## Notes and Other Information
- Implements privilege-based access control for configuration changes
- Non-interactive sources bypass normal privilege checks (except PGC_S_CLIENT)
- Returns different codes to distinguish validation failures from operational constraints
- Supports dry-run mode via changeVal parameter for validation-only operations
- Used extensively throughout PostgreSQL for configuration management
- Integrates with the transaction system through the action parameter
- Part of PostgreSQL's Grand Unified Configuration (GUC) system
- The is_reload parameter handles special cases during process startup and configuration reloading

## Simplified Source

```c
// Simplified version of set_config_option
int set_config_option(const char *name, const char *value,
                      GucContext context, GucSource source,
                      GucAction action, bool changeVal, int elevel,
                      bool is_reload) {
    Oid srole;

    // Privilege determination: Interactive sources use current user privileges,
    // non-interactive sources get superuser privileges (except PGC_S_CLIENT)
    if (source >= PGC_S_INTERACTIVE || source == PGC_S_CLIENT) {
        srole = GetUserId();
    } else {
        srole = BOOTSTRAP_SUPERUSERID;
    }

    // Delegate to the main implementation function
    return set_config_with_handle(name, NULL, value,
                                  context, source, srole,
                                  action, changeVal, elevel,
                                  is_reload);
}
```

Key simplifications made:
- Removed detailed comment block for brevity
- Focused on the two core operations: privilege determination and delegation
- Preserved the essential security logic that distinguishes interactive vs non-interactive sources
- Maintained the function's primary purpose as a privilege-checking wrapper
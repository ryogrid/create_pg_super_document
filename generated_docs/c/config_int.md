# config_int

## Location
[src/include/utils/guc_tables.h:212-227](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/guc_tables.h#L212-L227)

## Overview
A specialized GUC variable structure for integer-type configuration parameters, extending the base config_generic structure with integer-specific fields and validation hooks.

## Definition
```c
struct config_int
{
    struct config_generic gen;
    /* constant fields, must be set correctly in initial value: */
    int            *variable;
    int             boot_val;
    int             min;
    int             max;
    GucIntCheckHook check_hook;
    GucIntAssignHook assign_hook;
    GucShowHook     show_hook;
    /* variable fields, initialized at runtime: */
    int             reset_val;
    void           *reset_extra;
};
```

## Detailed Description
The config_int structure represents integer-type GUC variables in PostgreSQL's configuration system. It embeds a config_generic structure as its first member, enabling polymorphic operations, while adding integer-specific functionality including range validation, custom validation hooks, and assignment callbacks. The structure maintains both the current value (through the generic fields) and the reset value for RESET commands. The hook system allows for complex validation logic and side effects when integer variables are modified.

## Parameters / Member Variables
### Inherited from config_generic:
- `gen`: Base structure containing common GUC variable fields

### Constant Fields (set at initialization):
- `variable`: Pointer to the actual integer variable being controlled
- `boot_val`: Initial/boot-time value of the variable
- `min`: Minimum allowed value for the variable
- `max`: Maximum allowed value for the variable
- `check_hook`: Function called to validate new values (signature: bool (*)(int *newval, void **extra, GucSource source))
- `assign_hook`: Function called when a new value is assigned (signature: void (*)(int newval, void *extra))
- `show_hook`: Function called to display the current value (signature: const char *(*)(void))

### Runtime Fields:
- `reset_val`: Value to restore when RESET command is executed
- `reset_extra`: Extra data associated with the reset value

## Dependencies
- Types referenced:
  - [config_generic](config_generic.md) (base structure for all GUC variables)
  - GucIntCheckHook (function pointer type for validation)
  - GucIntAssignHook (function pointer type for assignment)
  - GucShowHook (function pointer type for display)
- Used by:
  - [DefineCustomIntVariable](../D/DefineCustomIntVariable.md) (function to create custom integer GUC variables)
  - Various GUC management functions (build_guc_variables, check_GUC_init, etc.)
  - GUC system functions for validation, assignment, and display operations

## Notes and Other Information
This structure is part of PostgreSQL's type-safe GUC system where each data type has its own specialized structure. The hook system provides powerful extensibility, allowing custom validation logic (check_hook), side effects on assignment (assign_hook), and custom display formatting (show_hook). The range validation (min/max) provides basic bounds checking before custom validation hooks are called. Integer GUC variables are commonly used for configuration parameters like buffer sizes, timeouts, and various numeric thresholds throughout PostgreSQL.
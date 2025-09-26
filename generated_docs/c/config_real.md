# config_real

## Location
[src/include/utils/guc_tables.h:228-253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/guc_tables.h#L228-L253)

## Overview
The `config_real` struct represents a floating-point configuration parameter in PostgreSQL's Grand Unified Configuration (GUC) system, managing runtime parameters that accept double-precision values with optional min/max constraints.

## Definition
```c
struct config_real
{
    struct config_generic gen;
    /* constant fields, must be set correctly in initial value: */
    double     *variable;
    double      boot_val;
    double      min;
    double      max;
    GucRealCheckHook check_hook;
    GucRealAssignHook assign_hook;
    GucShowHook show_hook;
    /* variable fields, initialized at runtime: */
    double      reset_val;
    void       *reset_extra;
};
```

## Detailed Description
The `config_real` structure is one of the core data types in PostgreSQL's GUC (Grand Unified Configuration) system, specifically designed to handle configuration parameters that store floating-point values. It extends the generic configuration structure with real-number-specific fields including range validation, custom hooks for checking and assignment operations, and transaction-safe reset capabilities. This structure supports parameters like `effective_cache_size`, `random_page_cost`, and other performance tuning parameters that require decimal precision.

## Parameters / Member Variables
- `gen`: Base `config_generic` structure containing common fields like name, context, flags, and source information
- `variable`: Pointer to the actual double variable that stores the current parameter value
- `boot_val`: Default value used during database initialization and as fallback
- `min`: Minimum allowed value for range validation (can be -DBL_MAX for no lower bound)
- `max`: Maximum allowed value for range validation (can be DBL_MAX for no upper bound)
- `check_hook`: Optional function pointer for custom validation logic beyond range checking
- `assign_hook`: Optional function pointer called when the parameter value changes
- `show_hook`: Optional function pointer for custom display formatting of the parameter value
- `reset_val`: Stored value for transaction rollback and RESET command support
- `reset_extra`: Additional context data for reset operations, managed by hooks

## Dependencies
- Functions called/Symbols referenced:
  - config_generic
- Called from (representative examples):
  - extra_field_used
  - set_stack_value
  - build_guc_variables
  - InitializeOneGUCOption
  - DefineCustomRealVariable
  - call_real_check_hook

## Notes and Other Information
- Part of PostgreSQL's type-safe configuration system that prevents runtime type errors
- Supports transaction-safe parameter changes through reset_val mechanism
- Range validation is performed automatically before custom check_hook execution
- Used extensively for performance tuning parameters that require floating-point precision
- Hook functions provide extensibility for custom validation and side effects during parameter changes
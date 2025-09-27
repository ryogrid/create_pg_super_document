# InitializeOneGUCOption

## Location
[src/backend/utils/misc/guc.c:1646-1762](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L1646-L1762)

## Overview
Initializes a single GUC parameter to its compiled-in default value, properly setting up all metadata fields and executing validation and assignment hooks.

## Definition
```c
static void InitializeOneGUCOption(struct config_generic *gconf)
```

## Detailed Description
This function performs comprehensive initialization of an individual GUC parameter, setting up both the parameter's value and its associated metadata. The initialization process operates in two main phases: metadata setup and value initialization.

During metadata setup, the function initializes all status fields to their default states: source and reset_source to PGC_S_DEFAULT, context fields to PGC_INTERNAL, role fields to BOOTSTRAP_SUPERUSERID, and clears the stack, extra data, and file location tracking fields.

For value initialization, the function handles each parameter type (BOOL, INT, REAL, STRING, ENUM) with type-specific processing. It starts with the boot_val (compiled-in default), validates the value through appropriate check hooks, executes any assignment hooks, and finally sets both the current value and reset value. For string parameters, it performs proper memory allocation using guc_strdup to ensure proper lifetime management.

The function ensures that validation hooks are called even for boot values, allowing hooks to compute "extra" data structures needed for parameter operation.

## Parameters / Member Variables
- `gconf`: Pointer to the generic GUC configuration structure to initialize

## Dependencies
- Functions called/Symbols referenced:
  - [config_generic](../c/config_generic.md): Base structure for all GUC parameters
  - PGC_S_DEFAULT, PGC_INTERNAL: Configuration source and context constants
  - BOOTSTRAP_SUPERUSERID: Default role ID for system initialization
  - PGC_BOOL, PGC_INT, PGC_REAL, PGC_STRING, PGC_ENUM: Parameter type constants
  - config_bool, config_int, config_real, config_string, config_enum: Type-specific structures
  - [call_bool_check_hook](../c/call_bool_check_hook.md), call_int_check_hook, call_real_check_hook, call_string_check_hook, call_enum_check_hook: Type-specific validation functions
  - [guc_strdup](../g/guc_strdup.md): GUC-specific string duplication function
- Called from (representative examples):
  - [InitializeGUCOptions](InitializeGUCOptions.md): Main GUC initialization during startup
  - [define_custom_variable](../d/define_custom_variable.md): Custom parameter registration
  - [RestoreGUCState](../R/RestoreGUCState.md): State restoration during recovery

## Notes and Other Information
- Static function used only within the GUC subsystem
- Ensures both current and reset values are properly initialized
- Validates boot values through check hooks even though they should always be valid
- Handles memory allocation for string parameters using GUC memory context
- Sets up proper metadata for source tracking and role-based access control
- Critical for establishing a consistent baseline state for all GUC parameters
- Located in src/backend/utils/misc/guc.c:1646-1762

## Simplified Source

```c
// Simplified version of InitializeOneGUCOption
static void InitializeOneGUCOption(struct config_generic *gconf) {
    // Initialize common metadata fields to defaults
    gconf->status = 0;
    gconf->source = PGC_S_DEFAULT;
    gconf->reset_source = PGC_S_DEFAULT;
    gconf->scontext = PGC_INTERNAL;
    gconf->reset_scontext = PGC_INTERNAL;
    gconf->srole = BOOTSTRAP_SUPERUSERID;
    gconf->reset_srole = BOOTSTRAP_SUPERUSERID;
    gconf->stack = NULL;
    gconf->extra = NULL;
    gconf->last_reported = NULL;
    gconf->sourcefile = NULL;
    gconf->sourceline = 0;

    // Initialize value based on parameter type
    switch (gconf->vartype) {
        case PGC_BOOL:
            initialize_bool_parameter((struct config_bool *) gconf);
            break;
        case PGC_INT:
            initialize_int_parameter((struct config_int *) gconf);
            break;
        case PGC_REAL:
            initialize_real_parameter((struct config_real *) gconf);
            break;
        case PGC_STRING:
            initialize_string_parameter((struct config_string *) gconf);
            break;
        case PGC_ENUM:
            initialize_enum_parameter((struct config_enum *) gconf);
            break;
    }
}

// Helper: Initialize boolean parameter
static void initialize_bool_parameter(struct config_bool *conf) {
    bool newval = conf->boot_val;
    void *extra = NULL;

    // Validate through check hook
    if (!call_bool_check_hook(conf, &newval, &extra, PGC_S_DEFAULT, LOG))
        elog(FATAL, "failed to initialize %s to %d", conf->gen.name, (int) newval);

    // Apply assignment hook if present
    if (conf->assign_hook)
        conf->assign_hook(newval, extra);

    // Set both current and reset values
    *conf->variable = conf->reset_val = newval;
    conf->gen.extra = conf->reset_extra = extra;
}

// Helper: Initialize integer parameter
static void initialize_int_parameter(struct config_int *conf) {
    int newval = conf->boot_val;
    void *extra = NULL;

    // Boot values should already be validated, but assert for safety
    Assert(newval >= conf->min && newval <= conf->max);

    // Validate and set value similar to boolean case
    if (!call_int_check_hook(conf, &newval, &extra, PGC_S_DEFAULT, LOG))
        elog(FATAL, "failed to initialize %s to %d", conf->gen.name, newval);

    if (conf->assign_hook)
        conf->assign_hook(newval, extra);

    *conf->variable = conf->reset_val = newval;
    conf->gen.extra = conf->reset_extra = extra;
}

// Helper: Initialize string parameter
static void initialize_string_parameter(struct config_string *conf) {
    char *newval;
    void *extra = NULL;

    // Duplicate boot value string if present
    if (conf->boot_val != NULL)
        newval = guc_strdup(FATAL, conf->boot_val);
    else
        newval = NULL;

    // Validate and set value
    if (!call_string_check_hook(conf, &newval, &extra, PGC_S_DEFAULT, LOG))
        elog(FATAL, "failed to initialize %s to \"%s\"",
             conf->gen.name, newval ? newval : "");

    if (conf->assign_hook)
        conf->assign_hook(newval, extra);

    *conf->variable = conf->reset_val = newval;
    conf->gen.extra = conf->reset_extra = extra;
}
```

Key simplifications made:
- Extracted repetitive type-specific initialization into helper functions
- Consolidated the common pattern of validate-assign-set across all types
- Added explanatory comments for each major step
- Removed detailed error message formatting variations
- Focused on the core logic flow rather than low-level details
- Maintained the essential validation and assignment hook patterns
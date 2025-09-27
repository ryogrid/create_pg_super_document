# set_config_with_handle

## Location
[src/backend/utils/misc/guc.c:3408-3711](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L3408-L3711)

## Overview
Sets a configuration option to a given value with optional handle optimization for repeated settings of the same option.

## Definition

```c
struct config_generic *record;
```
## Detailed Description
This function is the core implementation for setting PostgreSQL configuration options (GUCs). It provides an optimized interface that accepts a handle parameter to avoid repeated hash table lookups when setting the same configuration option multiple times. The function performs comprehensive validation of the setting request including context checks, privilege verification, and parallel operation safety.

The function handles various configuration contexts (POSTMASTER, SIGHUP, BACKEND, etc.) and enforces appropriate restrictions based on the parameter's definition and the current execution environment. It supports different actions like setting, saving for transaction rollback, and handles security restrictions appropriately.

## Parameters / Member Variables
- : Name of the configuration parameter to set
- : Optional handle from get_config_handle() to avoid hash lookup (NULL for normal lookup)
- : String value to set the parameter to (NULL for reset)
- : Context in which the setting is being made (PGC_INTERNAL, PGC_POSTMASTER, etc.)
- : Source of the setting (file, command line, etc.)
- : Role ID for privilege checking
- : Action to perform (GUC_ACTION_SET, GUC_ACTION_SAVE, etc.)
- : Whether to actually change the value or just validate
- : Error level for reporting problems
- : Whether this is part of a configuration reload

## Dependencies
- Functions called/Symbols referenced:
  - [find_option](../f/find_option.md)
  - [IsInParallelMode](../I/IsInParallelMode.md)
  - [pg_parameter_aclcheck](../p/pg_parameter_aclcheck.md)
  - [InLocalUserIdChange](../I/InLocalUserIdChange.md)
  - [InSecurityRestrictedOperation](../I/InSecurityRestrictedOperation.md)
  - [config_generic](../c/config_generic.md)
  - GucContext, GucSource, GucAction enums
- Called from (representative examples):
  - [set_config_option](set_config_option.md)
  - [set_config_option_ext](set_config_option_ext.md)
  - [fmgr_security_definer](../f/fmgr_security_definer.md)

## Notes and Other Information
- Returns 1 on success, 0 on failure, -1 if setting was ignored due to lower priority source
- The handle parameter is designed for performance optimization when repeatedly setting the same configuration options
- Implements comprehensive security checks including parallel operation restrictions and privilege validation
- Handles different parameter contexts with appropriate validation and error reporting
- Part of PostgreSQL's Grand Unified Configuration (GUC) system located in src/backend/utils/misc/guc.c:3408-3711

## Simplified Source

```c
// Simplified version of set_config_with_handle
int set_config_with_handle(const char *name, config_handle *handle,
                          const char *value,
                          GucContext context, GucSource source, Oid srole,
                          GucAction action, bool changeVal, int elevel,
                          bool is_reload)
{
    struct config_generic *record;
    union config_var_val newval_union;
    void *newextra = NULL;
    bool prohibitValueChange = false;
    bool makeDefault;

    // Step 1: Set appropriate error level if not specified
    if (elevel == 0) {
        if (source == PGC_S_DEFAULT || source == PGC_S_FILE)
            elevel = IsUnderPostmaster ? DEBUG3 : LOG;
        else if (source == PGC_S_GLOBAL || source == PGC_S_DATABASE ||
                 source == PGC_S_USER || source == PGC_S_DATABASE_USER)
            elevel = WARNING;
        else
            elevel = ERROR;
    }

    // Step 2: Get configuration record (use handle or lookup by name)
    if (!handle) {
        record = find_option(name, true, false, elevel);
        if (record == NULL)
            return 0;
    } else {
        record = handle;
    }

    // Step 3: Check parallel operation constraints
    if (IsInParallelMode() && changeVal && action != GUC_ACTION_SAVE &&
        (record->flags & GUC_ALLOW_IN_PARALLEL) == 0) {
        ereport(elevel, (errmsg("parameter \"%s\" cannot be set during a parallel operation", name)));
        return 0;
    }

    // Step 4: Validate context permissions (simplified main cases)
    switch (record->context) {
        case PGC_INTERNAL:
            if (context != PGC_INTERNAL) {
                ereport(elevel, (errmsg("parameter \"%s\" cannot be changed", name)));
                return 0;
            }
            break;

        case PGC_POSTMASTER:
            if (context == PGC_SIGHUP) {
                prohibitValueChange = true;  // Will check value later
            } else if (context != PGC_POSTMASTER) {
                ereport(elevel, (errmsg("parameter \"%s\" cannot be changed without restarting the server", name)));
                return 0;
            }
            break;

        case PGC_SUSET:
            if (context == PGC_USERSET || context == PGC_BACKEND) {
                // Check user privileges
                if (pg_parameter_aclcheck(name, srole, ACL_SET) != ACLCHECK_OK) {
                    ereport(elevel, (errmsg("permission denied to set parameter \"%s\"", name)));
                    return 0;
                }
            }
            break;

        case PGC_USERSET:
            // Always allowed
            break;
    }

    // Step 5: Check security restrictions
    if (record->flags & GUC_NOT_WHILE_SEC_REST) {
        if (InLocalUserIdChange() || InSecurityRestrictedOperation()) {
            ereport(elevel, (errmsg("cannot set parameter \"%s\" within security-restricted operation", name)));
            return 0;
        }
    }

    // Step 6: Check reset restrictions
    if (record->flags & GUC_NO_RESET) {
        if (value == NULL || action == GUC_ACTION_SAVE) {
            ereport(elevel, (errmsg("parameter \"%s\" cannot be reset/saved", name)));
            return 0;
        }
    }

    // Step 7: Determine if we should set defaults
    makeDefault = changeVal && (source <= PGC_S_OVERRIDE) &&
                  ((value != NULL) || source == PGC_S_DEFAULT);

    // Step 8: Check source priority
    if (record->source > source) {
        if (changeVal && !makeDefault) {
            return -1;  // Ignored due to higher priority source
        }
        changeVal = false;
    }

    // Step 9: Process value based on parameter type
    switch (record->vartype) {
        case PGC_BOOL:
            // Parse/validate boolean value, check hooks, update variable
            parse_and_validate_bool_value(record, name, value, &newval_union, &newextra);
            if (changeVal) {
                update_bool_variable(record, newval_union.boolval, newextra, action, makeDefault);
            }
            break;

        case PGC_INT:
            // Parse/validate integer value, check hooks, update variable
            parse_and_validate_int_value(record, name, value, &newval_union, &newextra);
            if (changeVal) {
                update_int_variable(record, newval_union.intval, newextra, action, makeDefault);
            }
            break;

        case PGC_REAL:
            // Parse/validate real value, check hooks, update variable
            parse_and_validate_real_value(record, name, value, &newval_union, &newextra);
            if (changeVal) {
                update_real_variable(record, newval_union.realval, newextra, action, makeDefault);
            }
            break;

        case PGC_STRING:
            // Parse/validate string value, check hooks, update variable
            parse_and_validate_string_value(record, name, value, &newval_union, &newextra);
            if (changeVal) {
                update_string_variable(record, newval_union.stringval, newextra, action, makeDefault);
                // Special handling for session_authorization
                if (strcmp(record->name, "session_authorization") == 0) {
                    set_config_with_handle("role", NULL, value ? "none" : NULL,
                                         context, source, srole, action, true, elevel, false);
                }
            }
            break;

        case PGC_ENUM:
            // Parse/validate enum value, check hooks, update variable
            parse_and_validate_enum_value(record, name, value, &newval_union, &newextra);
            if (changeVal) {
                update_enum_variable(record, newval_union.enumval, newextra, action, makeDefault);
            }
            break;
    }

    // Step 10: Mark for reporting if needed
    if (changeVal && (record->flags & GUC_REPORT) && !(record->status & GUC_NEEDS_REPORT)) {
        record->status |= GUC_NEEDS_REPORT;
        slist_push_head(&guc_report_list, &record->report_link);
    }

    return changeVal ? 1 : -1;
}
```

Key simplifications made:
- Consolidated repetitive parameter validation logic across types
- Abstracted type-specific parsing/validation into helper function calls
- Removed detailed memory management and error cleanup for clarity
- Simplified the complex fallthrough logic in context checking
- Focused on the main execution flow rather than edge cases
- Consolidated similar validation patterns across different parameter types
# GetConfigOptionByName

## Location
[src/backend/utils/misc/guc.c:5440-5472](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L5440-L5472)

## Overview
Retrieves the current value of a GUC configuration variable by name, with optional canonical name output and permission checking, returning a palloc'd string representation of the value.

## Definition
```c
char *GetConfigOptionByName(const char *name, const char **varname, bool missing_ok)
```

## Detailed Description
This function provides a programmatic interface to query PostgreSQL configuration variables by name. It performs comprehensive validation including variable existence checking, permission verification, and privilege enforcement. The function returns the string representation of the variable's current value, using the same formatting logic as the SHOW command.

The function handles both built-in and custom GUC variables, applying proper access control to ensure only authorized users can examine certain sensitive configuration parameters. If successful, it returns a newly allocated string containing the variable's value that must be freed by the caller.

## Parameters / Member Variables
- `name`: The name of the configuration variable to query
- `varname`: Optional output parameter that receives the canonical name of the variable (can be NULL)
- `missing_ok`: If true, return NULL for non-existent variables; if false, throw an error

## Dependencies
- Functions called/Symbols referenced:
  - [find_option](../f/find_option.md)
  - [ConfigOptionIsVisible](../C/ConfigOptionIsVisible.md)
  - [ShowGUCOption](../S/ShowGUCOption.md)
  - [config_generic](../c/config_generic.md)
- Called from (representative examples):
  - [ExplainPrintSettings](../E/ExplainPrintSettings.md) (explain.c:828, explain.c:853)
  - [ExtractSetVariableArgs](../E/ExtractSetVariableArgs.md) (guc_funcs.c:174)
  - [set_config_by_name](../s/set_config_by_name.md) (guc_funcs.c:371)
  - [GetPGVariableResultDesc](GetPGVariableResultDesc.md) (guc_funcs.c:414)
  - [ShowGUCConfigOption](../S/ShowGUCConfigOption.md) (guc_funcs.c:436)
  - [show_config_by_name](../s/show_config_by_name.md) (guc_funcs.c:813)
  - [show_config_by_name_missing_ok](../s/show_config_by_name_missing_ok.md) (guc_funcs.c:832)

## Notes and Other Information
The returned string is allocated with palloc() and must be freed by the caller using pfree(). The function enforces PostgreSQL's privilege system, requiring appropriate permissions to examine certain configuration variables. The varname parameter, if provided, receives a pointer to the canonical variable name from the GUC registry (this pointer should not be freed). The function respects the missing_ok parameter to provide flexible error handling for applications that need to handle non-existent variables gracefully.

## Simplified Source

```c
// Simplified version of GetConfigOptionByName
char *GetConfigOptionByName(const char *name, const char **varname, bool missing_ok) {
    struct config_generic *record;

    // Find the configuration option
    record = find_option(name, false, missing_ok, ERROR);
    if (record == NULL) {
        if (varname)
            *varname = NULL;
        return NULL;
    }

    // Check if user has permission to examine this option
    if (!ConfigOptionIsVisible(record))
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("permission denied to examine \"%s\"", name),
                 errdetail("Only roles with privileges of the \"%s\" role may examine this parameter.",
                           "pg_read_all_settings")));

    // Return canonical name if requested
    if (varname)
        *varname = record->name;

    // Return string representation of the option value
    return ShowGUCOption(record, true);
}
```

Key simplifications made:
- Preserved the essential variable lookup and validation logic
- Maintained the permission checking mechanism
- Simplified error reporting while keeping security checks
- Focused on the core configuration retrieval steps
- Preserved the optional varname output functionality
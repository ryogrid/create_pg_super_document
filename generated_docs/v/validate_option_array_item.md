# validate_option_array_item

## Location
[src/backend/utils/misc/guc.c:6716-6798](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L6716-L6798)

## Overview
Validates a proposed GUC parameter setting for array operations, checking both parameter validity and user permissions before allowing modification.

## Definition

```c
struct config_generic *gconf;
```
## Detailed Description
This static function validates GUC parameter operations in array contexts by performing comprehensive checks on parameter names, values, and user permissions. It handles three distinct cases: known GUC variables (validated normally with permission checks), unknown parameters that can become custom placeholders (allowed only for superusers), and invalid parameters (rejected unless specific conditions apply).

For known parameters, the function checks both the parameter context (USERSET vs SUSET) and user privileges. For custom variables, it allows superusers to create placeholders but restricts other users to prevent potential security issues. The function supports a reset mode for custom variables and can optionally skip permission errors instead of throwing exceptions.

## Parameters / Member Variables
- : Name of the GUC parameter to validate; must not be NULL
- : Proposed value for Add operations, or NULL for Delete/Reset operations
- : If true, return false instead of throwing permission errors

## Dependencies
- Functions called/Symbols referenced:
  - [config_generic](../c/config_generic.md)
  - [valid_custom_variable_name](valid_custom_variable_name.md)
  - [find_option](../f/find_option.md)
  - [superuser](../s/superuser.md)
  - [pg_parameter_aclcheck](../p/pg_parameter_aclcheck.md)
  - [set_config_option](../s/set_config_option.md)
  - [GetUserId](../G/GetUserId.md)
  - ereport
- Called from (representative examples):
  - [GUCArrayAdd](../G/GUCArrayAdd.md)
  - [GUCArrayDelete](../G/GUCArrayDelete.md)
  - [GUCArrayReset](../G/GUCArrayReset.md)
  - [GUCHashEntry](../G/GUCHashEntry.md)

## Notes and Other Information
- Returns true if validation succeeds, false if skipIfNoPermissions is true and user lacks permission
- Handles custom variable placeholders and validates custom variable names
- Uses set_config_option() in test mode (PGC_S_TEST) to validate values without applying changes
- Supports both USERSET parameters (user-modifiable) and SUSET parameters (superuser/privileged-only)
- Custom variables with invalid names are only allowed in reset contexts for cleanup purposes
- Permission checking considers both superuser status and explicit ACL_SET privileges on parameters
- Essential component of PostgreSQL's GUC security model for array-based configuration management

## Simplified Source

```c
static bool
validate_option_array_item(const char *name, const char *value, bool skipIfNoPermissions) {
    struct config_generic *gconf;
    bool reset_custom;

    // Check if this is a reset operation for a custom variable
    reset_custom = (!value && valid_custom_variable_name(name));

    // Find the configuration option, allowing placeholders if needed
    gconf = find_option(name, true, skipIfNoPermissions || reset_custom, ERROR);
    if (!gconf && !reset_custom) {
        // Unknown parameter and can't create placeholder
        return false;
    }

    // Handle custom/placeholder variables
    if (!gconf || gconf->flags & GUC_CUSTOM_PLACEHOLDER) {
        // Only superusers or users with ACL_SET can modify custom variables
        if (superuser() || pg_parameter_aclcheck(name, GetUserId(), ACL_SET) == ACLCHECK_OK)
            return true;
        if (skipIfNoPermissions)
            return false;
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to set parameter \"%s\"", name)));
    }

    // Check permissions for known parameters
    if (gconf->context == PGC_USERSET) {
        // User-settable parameters are always OK
    } else if (gconf->context == PGC_SUSET &&
               (superuser() || pg_parameter_aclcheck(name, GetUserId(), ACL_SET) == ACLCHECK_OK)) {
        // Superuser-only parameters require proper privileges
    } else if (skipIfNoPermissions) {
        return false;
    }

    // Test the parameter value for validity
    (void) set_config_option(name, value,
                            superuser() ? PGC_SUSET : PGC_USERSET,
                            PGC_S_TEST, GUC_ACTION_SET, false, 0, false);

    return true;
}
```
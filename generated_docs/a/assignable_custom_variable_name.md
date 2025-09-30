# assignable_custom_variable_name

## Location
[src/backend/utils/misc/guc.c:1123-1178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L1123-L1178)

## Overview
Determines whether an unrecognized configuration variable name is allowed to be SET by validating naming rules and checking against reserved namespace prefixes.

## Definition
```c
static bool assignable_custom_variable_name(const char *name, bool skip_errors, int elevel)
```

## Detailed Description
This function extends the validation performed by valid_custom_variable_name() by adding an additional layer of security: checking against reserved namespace prefixes. It serves as the authoritative validator for custom variable assignments, ensuring that user-defined variables don't conflict with extension-reserved namespaces.

The function performs validation in two main steps:
1. **Syntactic validation**: Calls valid_custom_variable_name() to ensure the name follows PostgreSQL's identifier rules
2. **Namespace reservation check**: Compares the variable's class (prefix before the first dot) against a list of reserved prefixes maintained by extensions

The function is separate from valid_custom_variable_name() because the reserved namespace check is not applied when reading configuration files, only during runtime SET operations. This allows configuration files to contain variables that might later be reserved by loaded extensions.

## Parameters / Member Variables
- `name`: The configuration parameter name to validate
- `skip_errors`: If true, return false on validation failure instead of reporting errors
- `elevel`: Error level to use when reporting validation failures (e.g., ERROR, WARNING)

## Dependencies
- Functions called/Symbols referenced:
  - strchr
  - [valid_custom_variable_name](../v/valid_custom_variable_name.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [errdetail](../e/errdetail.md)
  - lfirst
  - strlen
  - strncmp
- Data structures used:
  - reserved_class_prefix (global list)
  - ListCell
  - GUC_QUALIFIER_SEPARATOR
- Called from:
  - [find_option](../f/find_option.md) (src/backend/utils/misc/guc.c:1270)
  - [check_GUC_name_for_parameter_acl](../c/check_GUC_name_for_parameter_acl.md) (src/backend/utils/misc/guc.c:1418)
  - [AlterSystemSetConfigFile](../A/AlterSystemSetConfigFile.md) (src/backend/utils/misc/guc.c:4734)

## Notes and Other Information
- This is a static function, only accessible within the guc.c file
- Returns true if the variable name is valid and assignable, false otherwise
- Single-part names (without dots) are always rejected for custom variables
- Reserved prefixes are maintained in the reserved_class_prefix global list
- The function provides detailed error messages to help users understand naming requirements
- Error handling is flexible: callers can choose to suppress errors or specify the appropriate error level
- The separation from valid_custom_variable_name() allows different validation policies for file reading vs. runtime assignment

## Simplified Source

```c
static bool assignable_custom_variable_name(const char *name, bool skip_errors, int elevel)
{
    // Check if name contains a separator (required for custom variables)
    const char *separator = strchr(name, GUC_QUALIFIER_SEPARATOR);

    if (separator != NULL)
    {
        size_t class_length = separator - name;
        ListCell *lc;

        // First check syntax rules
        if (!valid_custom_variable_name(name))
        {
            if (!skip_errors)
                ereport(elevel, (errcode(ERRCODE_INVALID_NAME),
                               errmsg("invalid configuration parameter name \"%s\"", name),
                               errdetail("Custom parameter names must be two or more simple identifiers separated by dots.")));
            return false;
        }

        // Check against reserved prefixes
        foreach(lc, reserved_class_prefix)
        {
            const char *reserved_prefix = lfirst(lc);

            if (strlen(reserved_prefix) == class_length &&
                strncmp(name, reserved_prefix, class_length) == 0)
            {
                if (!skip_errors)
                    ereport(elevel, (errcode(ERRCODE_INVALID_NAME),
                                   errmsg("invalid configuration parameter name \"%s\"", name),
                                   errdetail("\"%s\" is a reserved prefix.", reserved_prefix)));
                return false;
            }
        }

        // Valid custom variable name
        return true;
    }

    // Single-part names are not allowed for custom variables
    if (!skip_errors)
        ereport(elevel, (errcode(ERRCODE_UNDEFINED_OBJECT),
                       errmsg("unrecognized configuration parameter \"%s\"", name)));
    return false;
}
```
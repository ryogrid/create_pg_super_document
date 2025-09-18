# assignable_custom_variable_name

## Location
src/backend/utils/misc/guc.c: 1123 - 1178

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
  - valid_custom_variable_name
  - ereport
  - errcode
  - errmsg
  - errdetail
  - lfirst
  - strlen
  - strncmp
- Data structures used:
  - reserved_class_prefix (global list)
  - ListCell
  - GUC_QUALIFIER_SEPARATOR
- Called from:
  - find_option (src/backend/utils/misc/guc.c:1270)
  - check_GUC_name_for_parameter_acl (src/backend/utils/misc/guc.c:1418)
  - AlterSystemSetConfigFile (src/backend/utils/misc/guc.c:4734)

## Notes and Other Information
- This is a static function, only accessible within the guc.c file
- Returns true if the variable name is valid and assignable, false otherwise
- Single-part names (without dots) are always rejected for custom variables
- Reserved prefixes are maintained in the reserved_class_prefix global list
- The function provides detailed error messages to help users understand naming requirements
- Error handling is flexible: callers can choose to suppress errors or specify the appropriate error level
- The separation from valid_custom_variable_name() allows different validation policies for file reading vs. runtime assignment
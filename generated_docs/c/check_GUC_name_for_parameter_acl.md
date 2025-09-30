# check_GUC_name_for_parameter_acl

## Location
[src/backend/utils/misc/guc.c:1412-1437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L1412-L1437)

## Overview
Validates whether a given parameter name is eligible for creating a pg_parameter_acl entry, ensuring the name refers to either an existing GUC parameter or a valid custom GUC name pattern.

## Definition
```c
void check_GUC_name_for_parameter_acl(const char *name)
```

## Detailed Description
This function performs validation checks on a parameter name before allowing the creation of a pg_parameter_acl entry. The function implements a two-stage validation process: first checking if the name corresponds to an existing GUC (Grand Unified Configuration) parameter, and if not, verifying that it follows the valid pattern for custom GUC variables. This ensures that ACL entries are only created for legitimate parameter names, preventing the creation of access control entries for invalid or malformed parameter names.

The function serves as a gatekeeper for the parameter ACL system, maintaining data integrity by rejecting invalid parameter names that could cause issues in the configuration management system.

## Parameters / Member Variables
- `name`: The parameter name to validate for ACL entry creation (can be in canonical or non-canonical form)

## Dependencies
- Functions called/Symbols referenced:
  - [find_option](../f/find_option.md): Searches for an existing GUC parameter by name
  - DEBUG5: Log level constant used for debug logging
  - [assignable_custom_variable_name](../a/assignable_custom_variable_name.md): Validates custom GUC variable name patterns
- Called from (representative examples):
  - [ParameterAclCreate](../P/ParameterAclCreate.md): Creates new parameter ACL entries
  - EmitWarningsOnPlaceholders: Warning system for placeholder parameters

## Notes and Other Information
- The function accepts both canonical and non-canonical parameter names
- Throws an error (via assignable_custom_variable_name) if the name is neither an existing GUC nor a valid custom GUC pattern
- Custom GUC names must follow specific naming conventions (typically containing a dot separator)
- This validation is crucial for maintaining the integrity of the parameter ACL system
- Located in src/backend/utils/misc/guc.c:1412-1437

## Simplified Source

```c
void
check_GUC_name_for_parameter_acl(const char *name)
{
    // Check if the GUC parameter already exists
    if (find_option(name, false, true, DEBUG5) != NULL)
        return;

    // If not found, validate that it's a valid custom GUC name pattern
    (void) assignable_custom_variable_name(name, false, ERROR);
}
```
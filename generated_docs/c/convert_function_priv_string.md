# convert_function_priv_string

## Location
[src/backend/utils/adt/acl.c:3577-3604](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3577-L3604)

## Overview
Converts a text string representation of function privileges into the corresponding AclMode bitmask value.

## Definition
```c
static AclMode convert_function_priv_string(text *priv_type_text)
```

## Detailed Description
This is a static helper function used by all has_function_privilege functions to convert textual privilege names into their corresponding AclMode bitmask values. It defines a privilege mapping table specific to functions and delegates the actual conversion to the generic convert_any_priv_string function. The function supports both the basic 'EXECUTE' privilege and the grant option variant 'EXECUTE WITH GRANT OPTION' for functions.

## Parameters / Member Variables
- `priv_type_text`: A text pointer containing the privilege name to be converted (e.g., 'EXECUTE')

## Dependencies
- Functions called/Symbols referenced:
  - [convert_any_priv_string](convert_any_priv_string.md)(): Generic privilege string conversion function (already processed)
  - priv_map: Structure type for privilege mapping tables
  - ACL_EXECUTE: Bitmask constant for execute privilege
  - ACL_GRANT_OPTION_FOR(): Macro to create grant option bitmask for a given privilege
- Called from (representative examples):
  - [has_function_privilege_name_name](../h/has_function_privilege_name_name.md)
  - [has_function_privilege_name](../h/has_function_privilege_name.md)
  - [has_function_privilege_name_id](../h/has_function_privilege_name_id.md)
  - [has_function_privilege_id](../h/has_function_privilege_id.md)
  - [has_function_privilege_id_name](../h/has_function_privilege_id_name.md)
  - [has_function_privilege_id_id](../h/has_function_privilege_id_id.md)

## Notes and Other Information
- This is a static function, only visible within the acl.c file
- Defines function-specific privilege mapping with only EXECUTE privileges supported
- Uses a static privilege mapping table for efficient lookups
- Part of the support routines for the has_function_privilege family of functions
- Leverages the generic convert_any_priv_string function for actual string parsing and conversion
- Located in src/backend/utils/adt/acl.c:3577-3604
# convert_type_priv_string

## Location
src/backend/utils/adt/acl.c: 4587 - 4614

## Overview
Converts a text string representing type privileges to the corresponding AclMode bitmask value.

## Definition
```c
static AclMode convert_type_priv_string(text *priv_type_text)
```

## Detailed Description
This static function serves as a support routine for the has_type_privilege family of functions. It converts text-based privilege strings into AclMode bitmask values for type privileges. The function defines a mapping table for type-specific privileges and delegates to the generic convert_any_priv_string function to perform the actual conversion. For types, PostgreSQL supports the "USAGE" privilege, which controls whether the type can be used in table definitions, function parameters, and other contexts.

## Parameters / Member Variables
- `priv_type_text`: A PostgreSQL text object containing the privilege string(s) to convert (e.g., "USAGE", "USAGE WITH GRANT OPTION")

## Dependencies
- Functions called/Symbols referenced:
  - convert_any_priv_string: Generic function that parses privilege strings using the provided mapping
  - ACL_USAGE: Constant representing the USAGE privilege bit
  - ACL_GRANT_OPTION_FOR: Macro that adds grant option to a privilege
  - priv_map: Type definition for privilege mapping structure
- Called from (representative examples):
  - has_type_privilege_name_name: Privilege check with role name and type name
  - has_type_privilege_name: Privilege check with current user and type name
  - has_type_privilege_name_id: Privilege check with role name and type OID
  - has_type_privilege_id: Privilege check with current user and type OID
  - has_type_privilege_id_name: Privilege check with role OID and type name
  - has_type_privilege_id_id: Privilege check with role OID and type OID

## Notes and Other Information
- This is a static (internal) function, not exposed outside acl.c
- Defines a privilege mapping table specific to types with "USAGE" and "USAGE WITH GRANT OPTION"
- Uses the generic convert_any_priv_string infrastructure for consistent privilege parsing
- Supports comma-separated privilege lists through the generic function
- Part of PostgreSQL's access control system for type privileges
- The USAGE privilege on types allows using the type in table definitions, function signatures, etc.
- Located in src/backend/utils/adt/acl.c:4587-4614
# convert_role_priv_string

## Location
src/backend/utils/adt/acl.c: 4854 - 4876

## Overview
This function converts a text string representing role privileges into an internal AclMode value, mapping role-specific privilege names to corresponding ACL bits for role access control operations.

## Definition


## Detailed Description
The `convert_role_priv_string` function is a static helper function that translates human-readable privilege strings into internal ACL mode values for role-based access control. It uses a predefined mapping table (role_priv_map) to convert strings like "USAGE", "MEMBER", "SET" and their grant/admin option variants into appropriate ACL bits. The function employs a special convention where ACL_CREATE is used to represent MEMBER privileges since there is no dedicated ACL bit for membership. This mapping convention is shared with the pg_role_aclcheck function.

## Parameters / Member Variables
- `priv_type_text` (text*): Text string containing the privilege name to convert (e.g., "USAGE", "MEMBER WITH ADMIN OPTION", "SET")

## Dependencies
- Functions called/Symbols referenced:
  - [convert_any_priv_string](convert_any_priv_string.md)
  - priv_map
  - ACL_USAGE
  - ACL_CREATE
  - ACL_SET
  - ACL_GRANT_OPTION_FOR
- Called from (representative examples):
  - [pg_has_role_name_name](../p/pg_has_role_name_name.md)
  - [pg_has_role_name](../p/pg_has_role_name.md)
  - [pg_has_role_name_id](../p/pg_has_role_name_id.md)
  - [pg_has_role_id](../p/pg_has_role_id.md)
  - [pg_has_role_id_name](../p/pg_has_role_id_name.md)
  - [pg_has_role_id_id](../p/pg_has_role_id_id.md)

## Notes and Other Information
- Uses a special convention where ACL_CREATE represents MEMBER privileges due to lack of dedicated ACL bit
- Supports grant/admin option variants for all privilege types
- The role_priv_map array defines the complete mapping between text strings and ACL mode values
- Function is static, indicating it's only used within the same compilation unit
- Part of the pg_has_role family of functions for role privilege checking
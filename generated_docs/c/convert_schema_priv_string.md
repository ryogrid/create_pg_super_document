# convert_schema_priv_string

## Location
src/backend/utils/adt/acl.c: 3977 - 4006

## Overview
Converts a text string representing schema privilege names to an AclMode bitmask value for PostgreSQL's access control system.

## Definition


## Detailed Description
This function parses a privilege string specific to schema objects and converts it to the corresponding AclMode bitmask. It uses a static privilege mapping table that defines the valid schema privilege names (CREATE and USAGE) and their corresponding ACL constants, including grant option variants. The function delegates the actual conversion logic to the generic  function.

The function supports both basic privileges and their "WITH GRANT OPTION" variants:
- CREATE: Allows creating objects within the schema
- USAGE: Allows accessing objects within the schema
- Both privileges can include grant options for delegation

## Parameters / Member Variables
- : Text input containing comma-separated privilege names to be converted to AclMode bitmask

## Dependencies
- Functions called/Symbols referenced:
  - [convert_any_priv_string](convert_any_priv_string.md)
  - ACL_CREATE
  - ACL_USAGE  
  - ACL_GRANT_OPTION_FOR
  - priv_map (struct type)
- Called from (representative examples):
  - [has_schema_privilege_name_name](../h/has_schema_privilege_name_name.md)
  - [has_schema_privilege_name](../h/has_schema_privilege_name.md)
  - [has_schema_privilege_name_id](../h/has_schema_privilege_name_id.md)
  - [has_schema_privilege_id](../h/has_schema_privilege_id.md)
  - [has_schema_privilege_id_name](../h/has_schema_privilege_id_name.md)
  - [has_schema_privilege_id_id](../h/has_schema_privilege_id_id.md)

## Notes and Other Information
- This is a static function, only accessible within the acl.c compilation unit
- Uses a compile-time static privilege mapping table for schema-specific privileges
- Part of PostgreSQL's privilege checking infrastructure for schema objects
- The privilege mapping covers the two main schema privileges: CREATE and USAGE
- Located in src/backend/utils/adt/acl.c:3977-4006
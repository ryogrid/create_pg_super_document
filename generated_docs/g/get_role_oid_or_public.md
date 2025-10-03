# get_role_oid_or_public

## Location
[src/backend/utils/adt/acl.c:5455-5470](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L5455-L5470)

## Overview
Extends get_role_oid to handle the special "public" pseudo-role by returning ACL_ID_PUBLIC for the string "public".

## Definition

```c
Oid
get_role_oid_or_public(const char *rolname)
```
## Detailed Description
This function is a wrapper around get_role_oid that provides special handling for PostgreSQL's "public" pseudo-role. When the input role name is exactly "public" (case-sensitive), the function returns the special constant ACL_ID_PUBLIC instead of performing a catalog lookup.

For all other role names, the function delegates to get_role_oid with missing_ok set to false, meaning it will throw an error if the role doesn't exist. This makes it a convenience function for contexts where the "public" pseudo-role should be recognized but missing regular roles should cause errors.

The "public" pseudo-role is a fundamental concept in PostgreSQL's permission system, representing all users/roles in the system. It doesn't exist as a regular role in pg_authid but is handled specially throughout the access control system.

## Parameters / Member Variables
- `*rolname`: The name of the role to look up, or "public" for the public pseudo-role
## Dependencies
- Functions called/Symbols referenced:
  - ACL_ID_PUBLIC (constant representing the public pseudo-role OID)
  - [get_role_oid](get_role_oid.md) (underlying role lookup function)
  - strcmp (standard C string comparison)
- Called from (representative examples):
  - [has_table_privilege_name_name](../h/has_table_privilege_name_name.md) (table privilege checking)
  - [has_sequence_privilege_name_name](../h/has_sequence_privilege_name_name.md) (sequence privilege checking)  
  - [has_any_column_privilege_name_name](../h/has_any_column_privilege_name_name.md) (column privilege checking)
  - [has_database_privilege_name_name](../h/has_database_privilege_name_name.md) (database privilege checking)
  - [has_foreign_data_wrapper_privilege_name_name](../h/has_foreign_data_wrapper_privilege_name_name.md) (FDW privilege checking)
  - [has_function_privilege_name_name](../h/has_function_privilege_name_name.md) (function privilege checking)
  - [has_language_privilege_name_name](../h/has_language_privilege_name_name.md) (language privilege checking)
  - [has_schema_privilege_name_name](../h/has_schema_privilege_name_name.md) (schema privilege checking)
  - [has_server_privilege_name_name](../h/has_server_privilege_name_name.md) (server privilege checking)
  - [has_tablespace_privilege_name_name](../h/has_tablespace_privilege_name_name.md) (tablespace privilege checking)
  - [has_type_privilege_name_name](../h/has_type_privilege_name_name.md) (type privilege checking)
  - [has_parameter_privilege_name_name](../h/has_parameter_privilege_name_name.md) (parameter privilege checking)

## Notes and Other Information
- The "public" string comparison is case-sensitive
- This function always uses missing_ok=false when calling get_role_oid, so missing regular roles will cause errors
- ACL_ID_PUBLIC is a special constant (typically 0) that represents the public pseudo-role throughout PostgreSQL's ACL system
- Widely used in has_*_privilege SQL functions that need to handle both regular roles and the public pseudo-role
- The public pseudo-role represents implicit permissions granted to all database users
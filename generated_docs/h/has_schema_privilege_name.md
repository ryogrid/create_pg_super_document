# has_schema_privilege_name

## Location
[src/backend/utils/adt/acl.c:3831-3854](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3831-L3854)

## Overview
Checks whether the current user has the specified privileges on a schema identified by schema name.

## Definition


## Detailed Description
This function is a simplified variant of the has_schema_privilege family that automatically uses the current user (obtained via GetUserId()) rather than requiring an explicit username parameter. It takes only two arguments: a schema name and a privilege type string. The function converts the schema name to a schema OID and the privilege string to an AclMode bitmask, then performs the privilege check for the current user. This is a convenience function for checking the current user's own privileges on a schema.

## Parameters / Member Variables
-  (text*): The name of the schema to check privileges against
-  (text*): A text string specifying the privilege type to check (e.g., "USAGE", "CREATE")

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md): Gets the OID of the current user/role
  - [convert_schema_name](../c/convert_schema_name.md): Converts schema name to schema OID  
  - [convert_schema_priv_string](../c/convert_schema_priv_string.md): Converts privilege string to AclMode bitmask
  - [object_aclcheck](../o/object_aclcheck.md): Performs the actual ACL privilege check against the object
  - AclResult: Enum type for ACL check results
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's privilege checking infrastructure for schemas
- One of several variants of has_schema_privilege, this one assumes current_user
- Uses object_aclcheck rather than object_aclcheck_ext, so it doesn't handle missing objects with NULL returns
- The function follows the standard PostgreSQL function calling convention using PG_FUNCTION_ARGS
- All variants are exposed at the SQL level under the same name "has_schema_privilege"
- Provides a convenient way to check current user's privileges without specifying a username
- Located in src/backend/utils/adt/acl.c:3831-3854
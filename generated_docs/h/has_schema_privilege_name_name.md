# has_schema_privilege_name_name

## Location
src/backend/utils/adt/acl.c: 3805 - 3830

## Overview
Checks whether a specific user (identified by username) has the specified privileges on a schema (identified by schema name).

## Definition


## Detailed Description
This function is part of the has_schema_privilege family of functions that check user privileges on database schemas. It takes three string/name arguments: a username, a schema name, and a privilege type string. The function converts the username to a role OID, the schema name to a schema OID, and the privilege string to an AclMode bitmask, then performs the actual privilege check using PostgreSQL's standard access control mechanisms. Unlike some variants, this function does not handle missing objects with NULL returns but relies on the underlying functions to throw errors for non-existent users or schemas.

## Parameters / Member Variables
-  (Name): The name of the role/user whose privileges are being checked
-  (text*): The name of the schema to check privileges against
-  (text*): A text string specifying the privilege type to check (e.g., "USAGE", "CREATE")

## Dependencies
- Functions called/Symbols referenced:
  - get_role_oid_or_public: Converts username to role OID, handling "public" role specially
  - convert_schema_name: Converts schema name to schema OID
  - convert_schema_priv_string: Converts privilege string to AclMode bitmask
  - object_aclcheck: Performs the actual ACL privilege check against the object
  - Name: PostgreSQL's name type for identifiers
  - AclResult: Enum type for ACL check results
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's privilege checking infrastructure for schemas
- One of several variants of has_schema_privilege that take different combinations of parameters
- Uses object_aclcheck rather than object_aclcheck_ext, so it doesn't handle missing objects with NULL returns
- The function follows the standard PostgreSQL function calling convention using PG_FUNCTION_ARGS
- All variants are exposed at the SQL level under the same name "has_schema_privilege"
- Located in src/backend/utils/adt/acl.c:3805-3830
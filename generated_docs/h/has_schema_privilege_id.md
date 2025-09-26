# has_schema_privilege_id

## Location
[src/backend/utils/adt/acl.c:3885-3912](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3885-L3912)

## Overview
Checks current user privileges on a schema given a schema OID and privilege type as text.

## Definition
Datum has_schema_privilege_id(PG_FUNCTION_ARGS)

## Detailed Description
This function is a PostgreSQL system function that verifies whether the current user has a particular privilege on a given schema. Unlike has_schema_privilege_name_id, this function automatically uses the current user context and takes only two parameters: a schema OID (Object Identifier) and a privilege type specified as text. The function retrieves the current user ID, converts the privilege string, and performs access control checking to determine if the user has the requested privilege on the schema.

The function handles cases where the schema might not exist by checking for missing objects and returns NULL in such cases. Otherwise, it returns a boolean value indicating whether the privilege check succeeded.

## Parameters / Member Variables
- schemaoid (Oid): The Object Identifier of the schema to check privileges against  
- priv_type_text (text*): Text representation of the privilege type (e.g. USAGE, CREATE)

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md)
  - [convert_schema_priv_string](../c/convert_schema_priv_string.md)
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md)
  - PG_GETARG_OID
  - PG_GETARG_TEXT_PP
  - [AclResult](../A/AclResult.md) (type)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- This is one of the PostgreSQL privilege checking functions accessible from SQL
- Automatically uses the current user context, eliminating the need to specify a username
- Returns NULL if the schema does not exist (is_missing flag)
- Uses the standard PostgreSQL function calling convention with PG_FUNCTION_ARGS
- Part of the Access Control List (ACL) system in PostgreSQL
- Located in src/backend/utils/adt/acl.c:3885-3912
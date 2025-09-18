# has_schema_privilege_name_id

## Location
src/backend/utils/adt/acl.c: 3855 - 3884

## Overview
Checks user privileges on a schema given a username, schema OID, and privilege type as text.

## Definition
Datum has_schema_privilege_name_id(PG_FUNCTION_ARGS)

## Detailed Description
This function is a PostgreSQL system function that verifies whether a specified user has a particular privilege on a given schema. It takes three parameters: a username (as Name type), a schema OID (Object Identifier), and a privilege type specified as text. The function performs role resolution, privilege string conversion, and access control checking to determine if the user has the requested privilege on the schema.

The function handles cases where the schema might not exist by checking for missing objects and returns NULL in such cases. Otherwise, it returns a boolean value indicating whether the privilege check succeeded.

## Parameters / Member Variables
- username (Name): The name of the user/role whose privileges are being checked
- schemaoid (Oid): The Object Identifier of the schema to check privileges against  
- priv_type_text (text*): Text representation of the privilege type (e.g. USAGE, CREATE)

## Dependencies
- Functions called/Symbols referenced:
  - get_role_oid_or_public
  - convert_schema_priv_string
  - object_aclcheck_ext
  - PG_GETARG_NAME
  - Name (type)
  - AclResult (type)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- This is one of the PostgreSQL privilege checking functions accessible from SQL
- Returns NULL if the schema does not exist (is_missing flag)
- Uses the standard PostgreSQL function calling convention with PG_FUNCTION_ARGS
- Part of the Access Control List (ACL) system in PostgreSQL
- Located in src/backend/utils/adt/acl.c:3855-3884
# has_schema_privilege_id_id

## Location
[src/backend/utils/adt/acl.c:3936-3964](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3936-L3964)

## Overview
Checks user privileges on a schema given a role ID, schema OID, and privilege type as text.

## Definition
Datum has_schema_privilege_id_id(PG_FUNCTION_ARGS)

## Detailed Description
This function is a PostgreSQL system function that verifies whether a specified role has a particular privilege on a given schema. It takes three parameters: a role OID (Object Identifier), a schema OID, and a privilege type specified as text. This is the most direct variant of the schema privilege checking functions, as both the role and schema are specified by their OIDs, eliminating the need for name resolution.

The function converts the privilege string to the appropriate mode and performs access control checking using object_aclcheck_ext to determine if the role has the requested privilege on the schema. It handles cases where the schema might not exist by checking for missing objects and returns NULL in such cases.

## Parameters / Member Variables
- roleid (Oid): The Object Identifier of the role whose privileges are being checked
- schemaoid (Oid): The Object Identifier of the schema to check privileges against
- priv_type_text (text*): Text representation of the privilege type (e.g. USAGE, CREATE)

## Dependencies
- Functions called/Symbols referenced:
  - [convert_schema_priv_string](../c/convert_schema_priv_string.md)
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md)
  - PG_GETARG_OID
  - PG_GETARG_TEXT_PP
  - AclResult (type)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- This is one of the PostgreSQL privilege checking functions accessible from SQL
- Most direct variant as it uses OIDs for both role and schema identification
- Returns NULL if the schema does not exist (is_missing flag)
- Uses object_aclcheck_ext for proper handling of missing objects
- Uses the standard PostgreSQL function calling convention with PG_FUNCTION_ARGS
- Part of the Access Control List (ACL) system in PostgreSQL
- Located in src/backend/utils/adt/acl.c:3936-3964
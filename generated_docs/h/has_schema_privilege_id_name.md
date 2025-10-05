# has_schema_privilege_id_name

## Location
[src/backend/utils/adt/acl.c:3913-3935](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3913-L3935)

## Overview
Checks user privileges on a schema given a role ID, schema name as text, and privilege type as text.

## Definition
Datum has_schema_privilege_id_name(PG_FUNCTION_ARGS)

## Detailed Description
This function is a PostgreSQL system function that verifies whether a specified role has a particular privilege on a given schema. It takes three parameters: a role OID (Object Identifier), a schema name as text, and a privilege type specified as text. The function first converts the schema name to its corresponding OID, then converts the privilege string to the appropriate mode, and finally performs access control checking to determine if the role has the requested privilege on the schema.

Unlike other variants in this family, this function does not handle missing schemas explicitly - it relies on convert_schema_name to handle schema name resolution, which may throw an error if the schema does not exist.

## Parameters / Member Variables
- roleid (Oid): The Object Identifier of the role whose privileges are being checked
- schemaname (text*): Text representation of the schema name
- priv_type_text (text*): Text representation of the privilege type (e.g. USAGE, CREATE)

## Dependencies
- Functions called/Symbols referenced:
  - [convert_schema_name](../c/convert_schema_name.md)
  - [convert_schema_priv_string](../c/convert_schema_priv_string.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - PG_GETARG_OID
  - PG_GETARG_TEXT_PP
  - [AclResult](../A/AclResult.md) (type)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- This is one of the PostgreSQL privilege checking functions accessible from SQL
- Uses object_aclcheck instead of object_aclcheck_ext, so it does not handle missing objects gracefully
- Schema name resolution may throw an error if the schema does not exist
- Uses the standard PostgreSQL function calling convention with PG_FUNCTION_ARGS
- Part of the Access Control List (ACL) system in PostgreSQL
- Located in src/backend/utils/adt/acl.c:3913-3935

## Simplified Source

```c
Datum
has_schema_privilege_id_name(PG_FUNCTION_ARGS)
{
    Oid        roleid = PG_GETARG_OID(0);
    text      *schemaname = PG_GETARG_TEXT_PP(1);
    text      *priv_type_text = PG_GETARG_TEXT_PP(2);
    Oid        schemaoid;
    AclMode    mode;
    AclResult  aclresult;

    // Convert schema name to schema OID
    schemaoid = convert_schema_name(schemaname);

    // Convert privilege string to access mode
    mode = convert_schema_priv_string(priv_type_text);

    // Check if role has the specified privilege on the schema
    aclresult = object_aclcheck(NamespaceRelationId, schemaoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}
```
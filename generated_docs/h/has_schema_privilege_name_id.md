# has_schema_privilege_name_id

## Location
[src/backend/utils/adt/acl.c:3855-3884](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3855-L3884)

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
  - [get_role_oid_or_public](../g/get_role_oid_or_public.md)
  - [convert_schema_priv_string](../c/convert_schema_priv_string.md)
  - [object_aclcheck_ext](../o/object_aclcheck_ext.md)
  - PG_GETARG_NAME
  - Name (type)
  - [AclResult](../A/AclResult.md) (type)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- This is one of the PostgreSQL privilege checking functions accessible from SQL
- Returns NULL if the schema does not exist (is_missing flag)
- Uses the standard PostgreSQL function calling convention with PG_FUNCTION_ARGS
- Part of the Access Control List (ACL) system in PostgreSQL
- Located in src/backend/utils/adt/acl.c:3855-3884

## Simplified Source

```c
Datum
has_schema_privilege_name_id(PG_FUNCTION_ARGS)
{
    Name       username = PG_GETARG_NAME(0);
    Oid        schemaoid = PG_GETARG_OID(1);
    text      *priv_type_text = PG_GETARG_TEXT_PP(2);
    Oid        roleid;
    AclMode    mode;
    AclResult  aclresult;
    bool       is_missing = false;

    // Convert username to role OID
    roleid = get_role_oid_or_public(NameStr(*username));

    // Convert privilege string to access mode
    mode = convert_schema_priv_string(priv_type_text);

    // Check privilege with extended version that handles missing objects
    aclresult = object_aclcheck_ext(NamespaceRelationId, schemaoid,
                                    roleid, mode, &is_missing);

    // Return NULL if schema doesn't exist
    if (is_missing)
        PG_RETURN_NULL();

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}
```
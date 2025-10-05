# has_schema_privilege_name_name

## Location
[src/backend/utils/adt/acl.c:3805-3830](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3805-L3830)

## Overview
Checks whether a specific user (identified by username) has the specified privileges on a schema (identified by schema name).

## Definition

```c
Datum
has_schema_privilege_name_name(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is part of the has_schema_privilege family of functions that check user privileges on database schemas. It takes three string/name arguments: a username, a schema name, and a privilege type string. The function converts the username to a role OID, the schema name to a schema OID, and the privilege string to an AclMode bitmask, then performs the actual privilege check using PostgreSQL's standard access control mechanisms. Unlike some variants, this function does not handle missing objects with NULL returns but relies on the underlying functions to throw errors for non-existent users or schemas.

## Parameters / Member Variables
-  (Name): The name of the role/user whose privileges are being checked
-  (text*): The name of the schema to check privileges against
-  (text*): A text string specifying the privilege type to check (e.g., "USAGE", "CREATE")

## Dependencies
- Functions called/Symbols referenced:
  - [get_role_oid_or_public](../g/get_role_oid_or_public.md): Converts username to role OID, handling "public" role specially
  - [convert_schema_name](../c/convert_schema_name.md): Converts schema name to schema OID
  - [convert_schema_priv_string](../c/convert_schema_priv_string.md): Converts privilege string to AclMode bitmask
  - [object_aclcheck](../o/object_aclcheck.md): Performs the actual ACL privilege check against the object
  - Name: PostgreSQL's name type for identifiers
  - [AclResult](../A/AclResult.md): Enum type for ACL check results
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's privilege checking infrastructure for schemas
- One of several variants of has_schema_privilege that take different combinations of parameters
- Uses object_aclcheck rather than object_aclcheck_ext, so it doesn't handle missing objects with NULL returns
- The function follows the standard PostgreSQL function calling convention using PG_FUNCTION_ARGS
- All variants are exposed at the SQL level under the same name "has_schema_privilege"
- Located in src/backend/utils/adt/acl.c:3805-3830

## Simplified Source

```c
Datum
has_schema_privilege_name_name(PG_FUNCTION_ARGS)
{
    Name       username = PG_GETARG_NAME(0);
    text      *schemaname = PG_GETARG_TEXT_PP(1);
    text      *priv_type_text = PG_GETARG_TEXT_PP(2);
    Oid        roleid;
    Oid        schemaoid;
    AclMode    mode;
    AclResult  aclresult;

    // Convert username to role OID
    roleid = get_role_oid_or_public(NameStr(*username));

    // Convert schema name to schema OID
    schemaoid = convert_schema_name(schemaname);

    // Convert privilege string to access mode
    mode = convert_schema_priv_string(priv_type_text);

    // Check if role has the specified privilege on the schema
    aclresult = object_aclcheck(NamespaceRelationId, schemaoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}
```
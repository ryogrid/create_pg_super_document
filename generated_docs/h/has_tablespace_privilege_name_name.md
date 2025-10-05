# has_tablespace_privilege_name_name

## Location
[src/backend/utils/adt/acl.c:4207-4232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4207-L4232)

## Overview
This function checks user privileges on a tablespace, taking a username (Name), tablespace name (text), and privilege name (text) as input parameters.

## Definition
```c
Datum has_tablespace_privilege_name_name(PG_FUNCTION_ARGS)
```

## Detailed Description
The `has_tablespace_privilege_name_name` function is part of PostgreSQL's built-in privilege checking system for tablespaces. It accepts three text-based arguments: a username, a tablespace name, and a privilege type. The function converts each of these text inputs into their corresponding OIDs and privilege bitmask, then performs an ACL check using PostgreSQL's standard object access control mechanism. This is one of several variants in the has_tablespace_privilege family that provide different combinations of input parameter types (names vs OIDs) for flexibility in SQL usage.

## Parameters / Member Variables
- `username`: Name type containing the username whose privileges are being checked
- `tablespacename`: Text representation of the tablespace name
- `priv_type_text`: Text representation of the privilege type to check (e.g., "CREATE")

## Dependencies
- Functions called/Symbols referenced:
  - [get_role_oid_or_public](../g/get_role_oid_or_public.md)
  - [convert_tablespace_name](../c/convert_tablespace_name.md)
  - [convert_tablespace_priv_string](../c/convert_tablespace_priv_string.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - Name (PostgreSQL data type)
  - [AclResult](../A/AclResult.md)
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
This function follows PostgreSQL's naming convention for privilege-checking functions where the suffix indicates the parameter types: "name_name" means both the user and object are specified by name rather than OID. The function uses `get_role_oid_or_public` which handles both regular usernames and the special "public" role. Tablespaces in PostgreSQL are storage locations that can be referenced by name or OID, and this function provides the name-based interface for privilege checking. The result is a boolean indicating whether the specified user has the requested privilege on the target tablespace.

## Simplified Source

```c
Datum
has_tablespace_privilege_name_name(PG_FUNCTION_ARGS)
{
    Name        username = PG_GETARG_NAME(0);
    text       *tablespacename = PG_GETARG_TEXT_PP(1);
    text       *priv_type_text = PG_GETARG_TEXT_PP(2);

    // Convert username to role OID (handles 'public' role)
    Oid roleid = get_role_oid_or_public(NameStr(*username));

    // Convert tablespace name to OID
    Oid tablespaceoid = convert_tablespace_name(tablespacename);

    // Convert privilege string to ACL mode
    AclMode mode = convert_tablespace_priv_string(priv_type_text);

    // Check access permissions
    AclResult aclresult = object_aclcheck(TableSpaceRelationId, tablespaceoid, roleid, mode);

    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}
```
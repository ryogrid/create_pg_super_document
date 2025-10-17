# pg_has_role_id_name

## Location
[src/backend/utils/adt/acl.c:4801-4823](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4801-L4823)

## Overview
Checks if a user (specified by OID) has specific privileges on a role (specified by name).

## Definition

```c
Datum
pg_has_role_id_name(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a mixed-parameter variant of the pg_has_role privilege checking system that accepts a user OID and a role name. It combines OID-based user identification with name-based role identification, which can be useful when the user OID is already known but the role needs to be specified by name. The function converts the role name to its corresponding OID and then performs the standard role privilege check.

## Parameters / Member Variables
-  (Oid roleid): The OID of the user whose privileges are being checked
-  (Name rolename): The name of the role on which privileges are being checked
-  (text priv_type_text): The privilege type as a text string (e.g., 'USAGE', 'MEMBER')

## Dependencies
- Functions called/Symbols referenced:
  - : Converts role name to role OID
  - : Converts privilege string to AclMode
  - : Performs the actual privilege check
  - : PostgreSQL macro to extract OID arguments
  - : PostgreSQL macro to extract Name arguments
  - : PostgreSQL macro to extract text arguments
  - : PostgreSQL macro to return boolean result
- Called from (representative examples):
  - SQL queries using pg_has_role(user_oid, rolename, privilege) function

## Notes and Other Information
- This function provides a hybrid approach where the user is specified by OID but the role by name
- Returns true if the specified user has the specified privilege on the role, false otherwise
- Located in src/backend/utils/adt/acl.c:4801-4823
- Completes the comprehensive pg_has_role function family that covers all combinations of name/OID parameters
- Useful when user OIDs are available from system catalogs but role names are more convenient for specification
- Less commonly used than other variants but provides completeness to the function family

## Simplified Source

```c
Datum pg_has_role_id_name(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);           // User OID to check
    Name rolename = PG_GETARG_NAME(1);       // Role name to check against
    text *priv_type_text = PG_GETARG_TEXT_PP(2); // Privilege type

    // Convert role name to OID
    Oid roleoid = get_role_oid(NameStr(*rolename), false);

    // Convert privilege string to access mode
    AclMode mode = convert_role_priv_string(priv_type_text);

    // Check if user has the specified privilege on the role
    AclResult result = pg_role_aclcheck(roleoid, roleid, mode);

    // Return true if access is granted
    PG_RETURN_BOOL(result == ACLCHECK_OK);
}
```
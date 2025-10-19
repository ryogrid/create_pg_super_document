# pg_has_role_id_id

## Location
[src/backend/utils/adt/acl.c:4824-4853](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L4824-L4853)

## Overview
This function checks whether a given user (roleid) has specific privileges on a target role (roleoid) by converting a text privilege string into an access control mode and performing an ACL check.

## Definition

```c
Datum
pg_has_role_id_id(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL system function that determines if a user has specific privileges on a role. It takes three arguments: a user role OID, a target role OID, and a text string specifying the privilege type. The function converts the privilege string to an internal access control mode using , then performs the actual privilege check using . It returns a boolean value indicating whether the privilege check succeeded.

## Parameters / Member Variables
-  (Oid): The OID of the user role whose privileges are being checked
-  (Oid): The OID of the target role on which privileges are being checked
-  (text*): Text string specifying the privilege type to check (e.g., "USAGE", "MEMBER")

## Dependencies
- Functions called/Symbols referenced:
  - [convert_role_priv_string](../c/convert_role_priv_string.md)
  - [pg_role_aclcheck](pg_role_aclcheck.md)
  - [AclResult](../A/AclResult.md)
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's access control system for role-based privileges
- Returns a boolean Datum that can be used in SQL queries
- The function follows PostgreSQL's standard function calling convention using PG_FUNCTION_ARGS
- Access control checks are performed through the ACL (Access Control List) subsystem

## Simplified Source

```c
Datum
pg_has_role_id_id(PG_FUNCTION_ARGS)
{
    // Extract function arguments
    Oid roleid = PG_GETARG_OID(0);           // User role checking privileges
    Oid roleoid = PG_GETARG_OID(1);          // Target role being checked
    text *priv_type_text = PG_GETARG_TEXT_PP(2); // Privilege type string

    // Convert privilege string to ACL mode
    AclMode mode = convert_role_priv_string(priv_type_text);

    // Check if the user role has specified privilege on target role
    AclResult aclresult = pg_role_aclcheck(roleoid, roleid, mode);

    // Return true if privilege check succeeded
    PG_RETURN_BOOL(aclresult == ACLCHECK_OK);
}
```
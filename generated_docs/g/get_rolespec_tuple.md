# get_rolespec_tuple

## Location
src/backend/utils/adt/acl.c: 5510 - 5555

## Overview
Converts a RoleSpec node to its corresponding pg_authid HeapTuple, providing access to the full role catalog entry rather than just the OID.

## Definition

```c
HeapTuple
get_rolespec_tuple(const RoleSpec *role)
```
## Detailed Description
This function is similar to get_rolespec_oid but returns the complete HeapTuple from the pg_authid system catalog instead of just the role's OID. This provides access to all role attributes such as role name, password, creation time, role options, and other metadata stored in pg_authid.

The function handles the same RoleSpec types as get_rolespec_oid:
- ROLESPEC_CSTRING: Looks up role by name using AUTHNAME cache
- ROLESPEC_CURRENT_ROLE/ROLESPEC_CURRENT_USER: Looks up current user by OID using AUTHOID cache  
- ROLESPEC_SESSION_USER: Looks up session user by OID using AUTHOID cache
- ROLESPEC_PUBLIC: Explicitly rejected with error

Unlike get_rolespec_oid, this function does not have a missing_ok parameter and will always throw an error if the role cannot be found. The caller is responsible for calling ReleaseSysCache() when done with the returned tuple.

This function is typically used when role DDL operations need access to complete role information, not just the role's identity.

## Parameters / Member Variables
- : Pointer to a RoleSpec node containing the role specification

## Dependencies
- Functions called/Symbols referenced:
  - RoleSpec (parser node type for role specifications)
  - ROLESPEC_CSTRING (enum value for string role names)
  - ROLESPEC_CURRENT_ROLE (enum value for CURRENT_ROLE keyword)
  - ROLESPEC_CURRENT_USER (enum value for CURRENT_USER keyword)
  - ROLESPEC_SESSION_USER (enum value for SESSION_USER keyword)
  - ROLESPEC_PUBLIC (enum value for PUBLIC keyword)
  - SearchSysCache1 (system cache lookup function)
  - CStringGetDatum (converts C string to PostgreSQL Datum)
  - ObjectIdGetDatum (converts OID to PostgreSQL Datum)
  - GetUserId (returns current user OID)
  - GetSessionUserId (returns session user OID)
  - HeapTupleIsValid (checks if tuple is valid)
  - Assert (assertion macro)
  - ereport/elog (error reporting functions)
- Called from (representative examples):
  - CreateRole (role creation command)
  - AlterRole (role alteration command)
  - AlterRoleSet (SET configuration for roles)
  - get_rolespec_name (extracts role name from RoleSpec)

## Notes and Other Information
- The caller MUST call ReleaseSysCache() on the returned tuple to avoid memory leaks
- Unlike get_rolespec_oid, this function has no missing_ok parameter and always errors on missing roles
- Uses different system cache indexes (AUTHNAME for name lookup, AUTHOID for OID lookup)
- PUBLIC is explicitly rejected with the same error message as get_rolespec_oid
- Internal system cache lookups for user/session user should never fail, so elog() is used instead of ereport()
- The returned tuple provides access to all pg_authid columns including rolname, rolsuper, rolinherit, rolcreaterole, etc.
- Essential for DDL operations that need to examine or modify role attributes beyond just identity
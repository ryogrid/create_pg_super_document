# get_role_oid

## Location
src/backend/utils/adt/acl.c: 5437 - 5454

## Overview
Looks up a role's OID given its name, with optional error handling for missing roles.

## Definition


## Detailed Description
This is a utility function that converts a role name (string) to its corresponding Object Identifier (OID) by performing a system catalog lookup. The function queries the pg_authid system catalog to find the role with the specified name.

The function provides flexible error handling through the missing_ok parameter. When missing_ok is false, the function will throw a PostgreSQL error if the role doesn't exist. When missing_ok is true, it silently returns InvalidOid instead of throwing an error, allowing the caller to handle the missing role case appropriately.

This is a fundamental function used throughout PostgreSQL's role and permission management system wherever role names need to be converted to OIDs for internal processing.

## Parameters / Member Variables
- : The name of the role to look up (null-terminated string)
- : Boolean flag controlling error behavior when role is not found
  - : Throw ERROR if role doesn't exist
  - : Return InvalidOid silently if role doesn't exist

## Dependencies
- Functions called/Symbols referenced:
  - GetSysCacheOid1 (system catalog cache lookup function)
  - CStringGetDatum (converts C string to PostgreSQL Datum)
  - OidIsValid (macro to check if OID is valid)
  - ereport (PostgreSQL error reporting function)
  - ERRCODE_UNDEFINED_OBJECT (error code constant)
- Called from (representative examples):
  - get_object_address_unqualified (object address resolution)
  - createdb (database creation)
  - CreateRole (role creation)
  - GrantRole (role granting)
  - is_member (HBA membership checking)
  - check_hba (HBA authentication)
  - aclparse (ACL parsing)
  - pg_has_role_* functions (role checking functions)
  - get_role_oid_or_public (extended role lookup)
  - get_rolespec_oid (rolespec conversion)

## Notes and Other Information
- Uses the AUTHNAME system cache for efficient role name lookups
- The function is widely used throughout PostgreSQL's security and role management subsystems
- InvalidOid is returned when missing_ok is true and the role doesn't exist
- Error message format: "role \"rolename\" does not exist" when missing_ok is false
- This function only looks up regular roles, not special pseudo-roles like PUBLIC
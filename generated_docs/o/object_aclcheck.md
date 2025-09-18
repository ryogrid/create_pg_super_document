# object_aclcheck

## Location
src/backend/catalog/aclchk.c: 3893 - 3902

## Overview
This is a public wrapper function that provides a simplified interface for checking a user's access privileges to any PostgreSQL database object.

## Definition
AclResult object_aclcheck(Oid classid, Oid objectid, Oid roleid, AclMode mode)

## Detailed Description
This function serves as a convenient public API for access control checking in PostgreSQL. It is a thin wrapper around object_aclcheck_ext that provides the core functionality without the extended error handling capabilities. The function delegates all permission checking logic to object_aclcheck_ext by passing NULL for the is_missing parameter, which means it will throw errors rather than handling missing objects gracefully.

## Parameters / Member Variables
- classid: The OID of the system catalog (pg_class, pg_proc, etc.) that contains the object being checked
- objectid: The OID of the specific object being checked for permissions
- roleid: The OID of the role whose permissions are being verified
- mode: The access mode/permissions being requested (AclMode type)

## Dependencies
- Functions called/Symbols referenced:
  - object_aclcheck_ext
- Called from (representative examples):
  - RangeVarGetAndCheckCreationNamespace
  - LookupExplicitNamespace  
  - CreateFunction
  - DefineIndex
  - DefineOperator
  - CreateSchemaCommand
  - DefineRelation
  - has_database_privilege_name_name (and many other privilege checking functions)

## Notes and Other Information
- This is the standard entry point for permission checking throughout the PostgreSQL codebase
- Unlike object_aclcheck_ext, this function will always throw errors for missing objects rather than returning gracefully
- Extensively used across the system for DDL operations, function calls, and privilege verification
- The function is declared in src/include/utils/acl.h and used throughout the backend
- Returns AclResult (typically ACLCHECK_OK or ACLCHECK_NO_PRIV)
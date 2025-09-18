# objectNamesToOids

## Location
[src/backend/catalog/aclchk.c:669-848](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L669-L848)

## Overview
Converts a list of object names of a specified type into a list of corresponding Object IDs (OIDs) by performing name resolution lookups in the PostgreSQL system catalogs.

## Definition


## Detailed Description
This static function performs comprehensive name-to-OID resolution for all PostgreSQL object types that support ACL-based permissions. It takes a list of object names and resolves each one to its corresponding OID through type-specific lookup functions. For most object types, the function performs straightforward catalog lookups using dedicated functions like get_database_oid() or get_namespace_oid(). More complex cases include: tables/sequences which use RangeVarGetRelid() to handle schema-qualified names; functions/procedures/routines which use LookupFuncWithArgs() to handle overloaded signatures; types/domains which use typenameTypeId() with type name parsing; large objects which parse OID strings directly and validate existence; and configuration parameters which have special logic to create pg_parameter_acl entries for GRANT operations while ignoring non-existent parameters during REVOKE operations. The function includes a notable limitation where it doesn't acquire locks on resolved objects, making it potentially vulnerable to concurrent DDL operations.

## Parameters / Member Variables
- : ObjectType enum specifying the type of objects being resolved (table, function, schema, etc.)
- : List of object names to be converted to OIDs, with format depending on object type
- : Boolean flag indicating whether this is for a GRANT operation (affects parameter ACL handling)

## Dependencies
- Functions called/Symbols referenced:
  - RangeVarGetRelid
  - [get_database_oid](../g/get_database_oid.md)
  - [typenameTypeId](../t/typenameTypeId.md)
  - makeTypeNameFromNameList
  - [LookupFuncWithArgs](../L/LookupFuncWithArgs.md)
  - [get_language_oid](../g/get_language_oid.md)
  - [oidparse](oidparse.md)
  - [LargeObjectExists](../L/LargeObjectExists.md)
  - [get_namespace_oid](../g/get_namespace_oid.md)
  - [get_tablespace_oid](../g/get_tablespace_oid.md)
  - [get_foreign_data_wrapper_oid](../g/get_foreign_data_wrapper_oid.md)
  - [get_foreign_server_oid](../g/get_foreign_server_oid.md)
  - [ParameterAclLookup](../P/ParameterAclLookup.md)
  - [ParameterAclCreate](../P/ParameterAclCreate.md)
  - CommandCounterIncrement
  - lappend_oid
  - strVal
  - lfirst
  - ereport
  - elog
- Types and structures:
  - ObjectType
  - [RangeVar](../R/RangeVar.md)
  - ObjectWithArgs
- Constants used:
  - All OBJECT_* type constants
  - NoLock
- Called from:
  - [ExecuteGrantStmt](../E/ExecuteGrantStmt.md)
  - InternalDefaultACL

## Notes and Other Information
- Contains an important concurrency limitation: no locks are acquired during name resolution, potentially causing GRANT/REVOKE failures if objects are modified concurrently
- Configuration parameters receive special treatment where GRANT operations will create pg_parameter_acl entries if they don't exist, while REVOKE operations silently skip non-existent parameters
- Uses CommandCounterIncrement() for parameter ACLs to make newly created entries visible within the same transaction
- Functions, procedures, and routines are handled similarly but use different OBJECT_* constants to distinguish their resolution context
- Large objects are unique in requiring direct OID parsing from strings rather than name-based lookup
- Tables and sequences share the same resolution path since both use the pg_class catalog
- Types and domains share resolution logic since both use the pg_type catalog
- The function supports all PostgreSQL object types that have ACL-based security, making it a comprehensive name resolution utility for the privilege system
- Error handling ensures that invalid object types are detected and non-existent objects (except parameters during REVOKE) cause appropriate errors
# LookupCreationNamespace

## Location
[src/backend/catalog/namespace.c:3428-3458](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L3428-L3458)

## Overview
Looks up a schema by name and verifies that the current user has CREATE privileges on it, handling the special case of the pg_temp temporary namespace.

## Definition

```c
Oid
LookupCreationNamespace(const char *nspname)
```
## Detailed Description
This function is the primary entry point for validating schema names when creating new database objects. It performs two main operations: locating the specified namespace by name and ensuring the current user has CREATE privileges on it. The function includes special handling for the "pg_temp" alias, which refers to the session's temporary namespace and may require initialization.

Unlike LookupExplicitNamespace (which checks for USAGE rights), this function specifically validates CREATE permissions, making it appropriate for DDL operations that will create new objects in the target schema. When dealing with the temporary namespace, it automatically initializes it if needed and may trigger a CommandCounterIncrement operation.

## Parameters / Member Variables
- : The name of the namespace/schema to look up and validate

## Dependencies
- Functions called/Symbols referenced:
  - [AccessTempTableNamespace](../A/AccessTempTableNamespace.md) (for pg_temp initialization)
  - [get_namespace_oid](../g/get_namespace_oid.md) (to resolve namespace name to OID)
  - [object_aclcheck](../o/object_aclcheck.md) (to verify CREATE permissions)
  - [aclcheck_error](../a/aclcheck_error.md) (to report permission errors)
  - [GetUserId](../G/GetUserId.md) (implicitly called for permission check)
- Called from (representative examples):
  - [ExecAlterObjectSchemaStmt](../E/ExecAlterObjectSchemaStmt.md)
  - [make_new_heap](../m/make_new_heap.md)
  - [AlterExtensionNamespace](../A/AlterExtensionNamespace.md)
  - [ImportForeignSchema](../I/ImportForeignSchema.md)
  - [AlterTypeNamespace](../A/AlterTypeNamespace.md)
  - RangeVarGetRelid

## Notes and Other Information
- Special case handling for "pg_temp" alias ensures temporary namespace is properly initialized
- May result in a CommandCounterIncrement operation when temp namespace creation/cleanup is needed
- Throws an error via aclcheck_error if the user lacks CREATE privileges on the target namespace
- Returns the namespace OID on successful validation
- Part of PostgreSQL's namespace resolution and permission checking infrastructure
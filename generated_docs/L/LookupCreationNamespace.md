# LookupCreationNamespace

## Location
src/backend/catalog/namespace.c: 3428 - 3458

## Overview
Looks up a schema by name and verifies that the current user has CREATE privileges on it, handling the special case of the pg_temp temporary namespace.

## Definition


## Detailed Description
This function is the primary entry point for validating schema names when creating new database objects. It performs two main operations: locating the specified namespace by name and ensuring the current user has CREATE privileges on it. The function includes special handling for the "pg_temp" alias, which refers to the session's temporary namespace and may require initialization.

Unlike LookupExplicitNamespace (which checks for USAGE rights), this function specifically validates CREATE permissions, making it appropriate for DDL operations that will create new objects in the target schema. When dealing with the temporary namespace, it automatically initializes it if needed and may trigger a CommandCounterIncrement operation.

## Parameters / Member Variables
- : The name of the namespace/schema to look up and validate

## Dependencies
- Functions called/Symbols referenced:
  - AccessTempTableNamespace (for pg_temp initialization)
  - get_namespace_oid (to resolve namespace name to OID)
  - object_aclcheck (to verify CREATE permissions)
  - aclcheck_error (to report permission errors)
  - GetUserId (implicitly called for permission check)
- Called from (representative examples):
  - ExecAlterObjectSchemaStmt
  - make_new_heap
  - AlterExtensionNamespace
  - ImportForeignSchema
  - AlterTypeNamespace
  - RangeVarGetRelid

## Notes and Other Information
- Special case handling for "pg_temp" alias ensures temporary namespace is properly initialized
- May result in a CommandCounterIncrement operation when temp namespace creation/cleanup is needed
- Throws an error via aclcheck_error if the user lacks CREATE privileges on the target namespace
- Returns the namespace OID on successful validation
- Part of PostgreSQL's namespace resolution and permission checking infrastructure
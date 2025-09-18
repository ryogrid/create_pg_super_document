# NamespaceCreate

## Location
src/backend/catalog/pg_namespace.c: 43 - 120

## Overview
Creates a new namespace (schema) in the PostgreSQL catalog with the given name and owner, handling both regular schemas and temporary schemas with appropriate dependency tracking.

## Definition


## Detailed Description
NamespaceCreate is the core function responsible for creating new namespaces (schemas) in PostgreSQL's catalog system. It performs comprehensive validation, inserts the new namespace record into the pg_namespace system catalog, and establishes all necessary dependencies. The function handles both regular schemas and temporary schemas, with special treatment for temporary schemas to prevent them from being linked as extension members and to skip default ACL processing. The function ensures proper locking, generates a unique OID for the namespace, and invokes post-creation hooks for extensibility.

## Parameters / Member Variables
- : The name of the namespace to be created; must not be NULL and must be unique within the database
- : The OID of the role that will own the new namespace
- : Boolean flag indicating whether this is a temporary schema; affects extension membership and ACL processing

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheExists1: Check for existing namespace with the same name
  - get_user_default_acl: Retrieve default ACL for the schema owner (skipped for temp schemas)
  - table_open: Open the pg_namespace catalog for modification
  - GetNewOidWithIndex: Generate a unique OID for the new namespace
  - namestrcpy: Copy the namespace name into a NameData structure
  - heap_form_tuple: Create the catalog tuple for insertion
  - CatalogTupleInsert: Insert the new namespace tuple into pg_namespace
  - recordDependencyOnOwner: Record ownership dependency
  - recordDependencyOnNewAcl: Record ACL-related dependencies
  - recordDependencyOnCurrentExtension: Record extension membership (skipped for temp schemas)
  - InvokeObjectPostCreateHook: Trigger post-creation hooks for extensions

- Called from (representative examples):
  - InitTempTableNamespace: Creates temporary schemas for backend sessions
  - CreateSchemaCommand: Implements the CREATE SCHEMA SQL command

## Notes and Other Information
- The function includes comprehensive error checking, ensuring the namespace name is provided and doesn't conflict with existing schemas
- Temporary schemas receive special handling: they are excluded from extension membership to prevent temporary tables created in extension scripts from making the temp schema part of the extension
- Default ACL processing is skipped for temporary schemas as it's not necessary for their intended use
- The function maintains proper catalog consistency through row-exclusive locking and transactional operations
- All dependency relationships are properly recorded to ensure cascade operations work correctly during schema drops
- Post-creation hooks allow extensions and other components to respond to schema creation events
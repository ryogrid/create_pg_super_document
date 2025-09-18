# get_namespace_oid

## Location
src/backend/catalog/namespace.c: 3535 - 3553

## Overview
Looks up a namespace (schema) by name and returns its OID, with optional error handling for missing namespaces.

## Definition


## Detailed Description
This function serves as the primary interface for resolving namespace names to their corresponding object identifiers (OIDs) in PostgreSQL's system catalog. It performs a direct lookup in the pg_namespace system catalog using the system cache for efficient access.

The function is designed to be flexible in error handling - callers can choose whether missing namespaces should trigger an error or simply return an invalid OID. This makes it suitable for both validation scenarios (where existence is required) and exploratory scenarios (where existence needs to be checked).

The function uses PostgreSQL's system cache (NAMESPACENAME) for efficient lookups, avoiding repeated direct table scans of pg_namespace. This is particularly important given that namespace lookups are frequent operations in SQL parsing and execution.

## Parameters / Member Variables
- `nspname`: The name of the namespace/schema to look up
- `missing_ok`: If false, throws an error when the namespace doesn't exist; if true, returns InvalidOid instead

## Dependencies
- Functions called/Symbols referenced:
  - GetSysCacheOid1 (to query the system cache)
  - CStringGetDatum (to convert string to Datum for cache lookup)
  - OidIsValid (to check if returned OID is valid)
  - ereport/ERROR (for error reporting when missing_ok is false)
- Called from (representative examples):
  - LookupCreationNamespace
  - LookupExplicitNamespace
  - QualifiedNameGetCreationNamespace
  - RangeVarGetCreationNamespace
  - CreateSchemaCommand
  - RenameSchema
  - Many other catalog and command functions

## Notes and Other Information
- Uses system cache NAMESPACENAME for efficient lookups
- Returns InvalidOid when namespace not found and missing_ok is true
- Throws ERRCODE_UNDEFINED_SCHEMA error when namespace not found and missing_ok is false
- Core utility function used throughout PostgreSQL's namespace resolution infrastructure
- Does not perform any permission checking - purely a name-to-OID resolution function
- Essential for DDL operations, schema management, and qualified name resolution
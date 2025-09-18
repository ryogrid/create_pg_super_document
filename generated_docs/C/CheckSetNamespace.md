# CheckSetNamespace

## Location
src/backend/catalog/namespace.c: 3459 - 3486

## Overview
Validates that namespace transitions are allowed by checking restrictions on temporary and TOAST schemas during object moves.

## Definition


## Detailed Description
This function enforces PostgreSQL's restrictions on moving database objects between certain special namespaces. It serves as a common validation point for ALTER ... SET SCHEMA operations, preventing potentially problematic namespace transitions that could break system invariants or cause operational issues.

The function specifically prohibits moving objects into or out of temporary namespaces (including temporary toast schemas) and the system TOAST namespace. These restrictions exist because temporary schemas have session-specific semantics and lifecycle management, while the TOAST namespace contains system-managed storage for large values that should not be directly manipulated by users.

## Parameters / Member Variables
- `oldNspOid`: The OID of the current/source namespace
- `nspOid`: The OID of the target/destination namespace

## Dependencies
- Functions called/Symbols referenced:
  - [isAnyTempNamespace](../i/isAnyTempNamespace.md) (to check if a namespace is temporary)
  - ereport/ERROR (for error reporting)
  - PG_TOAST_NAMESPACE (constant for TOAST schema OID)
- Called from (representative examples):
  - [AlterObjectNamespace_internal](../A/AlterObjectNamespace_internal.md)
  - [AlterTableNamespace](../A/AlterTableNamespace.md)  
  - [AlterTypeNamespaceInternal](../A/AlterTypeNamespaceInternal.md)
  - RangeVarGetRelid

## Notes and Other Information
- Throws ERRCODE_FEATURE_NOT_SUPPORTED errors when prohibited moves are attempted
- Part of PostgreSQL's schema management safety mechanisms
- Helps maintain the integrity of temporary and system-managed namespaces
- Used consistently across different ALTER ... SET SCHEMA implementations
- Does not return a value; either succeeds silently or throws an error
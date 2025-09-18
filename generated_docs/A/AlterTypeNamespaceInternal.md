# AlterTypeNamespaceInternal

## Location
src/backend/commands/typecmds.c: 4156 - 4311

## Overview
The core internal function that performs the actual namespace migration for PostgreSQL types, handling all type variants including composite types, domains, and arrays with comprehensive dependency management.

## Definition


## Detailed Description
AlterTypeNamespaceInternal is the workhorse function that performs the actual type namespace change operations. It handles the complete process including catalog updates, dependency tracking, constraint migration, and recursive processing of associated array types. The function distinguishes between different type categories (composite types, domains, table row types) and applies appropriate handling for each. It maintains referential integrity by updating both pg_type and pg_class catalogs for composite types and properly managing namespace dependencies.

## Parameters / Member Variables
- : OID of the type to be moved to the new namespace
- : OID of the target namespace where the type should be relocated
- : Boolean flag indicating if this is an internal recursive call for an array type
- : Boolean flag to silently skip table row types instead of erroring
- : Boolean flag to raise an error when encountering table row types (ignored if ignoreDependent is true)
- : ObjectAddresses structure tracking all objects moved during the operation to prevent duplicate processing

## Dependencies
- Functions called/Symbols referenced:
  - [object_address_present](../o/object_address_present.md)
  - SearchSysCacheCopy1
  - [CheckSetNamespace](../C/CheckSetNamespace.md)
  - SearchSysCacheExists2
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [AlterRelationNamespaceInternal](AlterRelationNamespaceInternal.md)
  - [AlterConstraintNamespaces](AlterConstraintNamespaces.md)
  - [changeDependencyFor](../c/changeDependencyFor.md)
  - InvokeObjectPostAlterHook
  - [heap_freetuple](../h/heap_freetuple.md)
  - [add_exact_object_address](../a/add_exact_object_address.md)
- Called from (representative examples):
  - [AlterTypeNamespace_oid](AlterTypeNamespace_oid.md)
  - [AlterTableNamespaceInternal](AlterTableNamespaceInternal.md)
  - [AlterTypeNamespaceInternal](AlterTypeNamespaceInternal.md) (recursive call)

## Notes and Other Information
- Automatically recurses to process associated array types when moving a base type
- Prevents duplicate processing by checking objsMoved before starting work
- Handles composite types by updating both pg_type and pg_class catalogs
- Migrates constraints for both composite types and domain types to the new namespace
- Updates schema dependencies except for table row types and implicit arrays
- Returns InvalidOid if no action was taken, otherwise returns the old namespace OID
- Invokes post-alter hooks to notify other subsystems of the change
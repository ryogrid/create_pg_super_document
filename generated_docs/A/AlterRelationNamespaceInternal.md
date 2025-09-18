# AlterRelationNamespaceInternal

## Location
[src/backend/commands/tablecmds.c:17315-17391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L17315-L17391)

## Overview
A core internal function that relocates a relation (table, index, sequence, etc.) from one namespace (schema) to another by updating the pg_class catalog entry and related dependency information.

## Definition


## Detailed Description
This function implements the core logic for moving a relation between namespaces. It operates on the pg_class catalog directly, updating the relnamespace field and managing associated dependencies. The function includes safeguards against duplicate relation names in the target namespace and tracks moved objects to prevent duplicate operations. It requires the caller to have already opened and write-locked the pg_class relation for thread safety.

## Parameters / Member Variables
- : Pre-opened and write-locked pg_class relation for catalog updates
- : Object identifier of the relation being moved
- : Object identifier of the source namespace
- : Object identifier of the destination namespace  
- : Boolean indicating whether to update schema dependency entries
- : Collection tracking objects already processed to prevent duplicates

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCacheLockedCopy1](../S/SearchSysCacheLockedCopy1.md)
  - [object_address_present](../o/object_address_present.md)
  - [get_relname_relid](../g/get_relname_relid.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [UnlockTuple](../U/UnlockTuple.md)
  - [changeDependencyFor](../c/changeDependencyFor.md)
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - InvokeObjectPostAlterHook
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [AlterTableNamespaceInternal](AlterTableNamespaceInternal.md)
  - [AlterIndexNamespaces](AlterIndexNamespaces.md)
  - [AlterSeqNamespaces](AlterSeqNamespaces.md)
  - [AlterTypeNamespaceInternal](AlterTypeNamespaceInternal.md)

## Notes and Other Information
- Checks for name conflicts in the target namespace before proceeding with the move
- Uses tuple locking mechanisms to ensure data consistency during catalog updates
- Fires post-alter hooks to notify other subsystems of the namespace change
- Handles cases where objects have already been moved or are already in the correct namespace
- Critical for implementing ALTER TABLE/INDEX/SEQUENCE SET SCHEMA operations
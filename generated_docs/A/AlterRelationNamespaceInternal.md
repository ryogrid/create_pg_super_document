# AlterRelationNamespaceInternal

## Location
src/backend/commands/tablecmds.c: 17315 - 17391

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
  - SearchSysCacheLockedCopy1
  - object_address_present
  - get_relname_relid
  - get_namespace_name
  - CatalogTupleUpdate
  - UnlockTuple
  - changeDependencyFor
  - add_exact_object_address
  - InvokeObjectPostAlterHook
  - heap_freetuple
- Called from (representative examples):
  - AlterTableNamespaceInternal
  - AlterIndexNamespaces
  - AlterSeqNamespaces
  - AlterTypeNamespaceInternal

## Notes and Other Information
- Checks for name conflicts in the target namespace before proceeding with the move
- Uses tuple locking mechanisms to ensure data consistency during catalog updates
- Fires post-alter hooks to notify other subsystems of the namespace change
- Handles cases where objects have already been moved or are already in the correct namespace
- Critical for implementing ALTER TABLE/INDEX/SEQUENCE SET SCHEMA operations
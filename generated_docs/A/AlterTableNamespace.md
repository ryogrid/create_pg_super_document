# AlterTableNamespace

## Location
src/backend/commands/tablecmds.c: 17207 - 17277

## Overview
AlterTableNamespace implements the ALTER TABLE SET SCHEMA command by moving a table and its dependent objects from one namespace (schema) to another while performing necessary validation and permission checks.

## Definition


## Detailed Description
This function handles the high-level logic for moving a table between schemas. It performs several important validation steps and delegates the actual namespace change to AlterTableNamespaceInternal. The function ensures that:

1. The target relation exists (or handles missing_ok gracefully)
2. Owned sequences cannot be moved independently from their owning tables
3. The target schema exists and the user has appropriate permissions
4. Common namespace change validations pass
5. All dependent objects are moved together atomically

The function uses AccessExclusiveLock to ensure exclusive access during the schema change operation. It maintains referential integrity by moving related objects together and validates ownership relationships for sequences.

## Parameters / Member Variables
- : AlterObjectSchemaStmt containing the relation reference, new schema name, and missing_ok flag
- : Optional output parameter to return the OID of the old schema

## Dependencies
- Functions called/Symbols referenced:
  - RangeVarGetRelidExtended
  - relation_open
  - RelationGetNamespace
  - sequenceIsOwned
  - get_rel_name
  - makeRangeVar
  - RangeVarGetAndCheckCreationNamespace
  - CheckSetNamespace
  - new_object_addresses
  - AlterTableNamespaceInternal
  - free_object_addresses
  - ObjectAddressSet
  - relation_close

- Called from (representative examples):
  - ExecAlterObjectSchemaStmt (main schema alteration dispatcher)

## Notes and Other Information
- Returns InvalidObjectAddress if the relation doesn't exist and missing_ok is true
- Prevents owned sequences from being moved independently of their owning table
- Uses AccessExclusiveLock during the entire operation to prevent concurrent modifications
- The actual namespace change work is delegated to AlterTableNamespaceInternal
- Maintains the lock on the relation until transaction commit
- Tracks moved objects using ObjectAddresses for proper dependency management
- Performs standard namespace change validations via CheckSetNamespace
- Returns an ObjectAddress pointing to the moved relation for dependency tracking
- If oldschema parameter is provided, it receives the OID of the original schema
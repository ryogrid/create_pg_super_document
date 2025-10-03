# AlterTableNamespace

## Location
[src/backend/commands/tablecmds.c:17207-17277](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L17207-L17277)

## Overview
AlterTableNamespace implements the ALTER TABLE SET SCHEMA command by moving a table and its dependent objects from one namespace (schema) to another while performing necessary validation and permission checks.

## Definition

```c
ObjectAddress
AlterTableNamespace(AlterObjectSchemaStmt *stmt, Oid *oldschema)
```
## Detailed Description
This function handles the high-level logic for moving a table between schemas. It performs several important validation steps and delegates the actual namespace change to AlterTableNamespaceInternal. The function ensures that:

1. The target relation exists (or handles missing_ok gracefully)
2. Owned sequences cannot be moved independently from their owning tables
3. The target schema exists and the user has appropriate permissions
4. Common namespace change validations pass
5. All dependent objects are moved together atomically

The function uses AccessExclusiveLock to ensure exclusive access during the schema change operation. It maintains referential integrity by moving related objects together and validates ownership relationships for sequences.

## Parameters / Member Variables
- `*stmt`: AlterObjectSchemaStmt containing the relation reference, new schema name, and missing_ok flag
- `*oldschema`: Optional output parameter to return the OID of the old schema
## Dependencies
- Functions called/Symbols referenced:
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md)
  - [relation_open](../r/relation_open.md)
  - RelationGetNamespace
  - [sequenceIsOwned](../s/sequenceIsOwned.md)
  - [get_rel_name](../g/get_rel_name.md)
  - [makeRangeVar](../m/makeRangeVar.md)
  - [RangeVarGetAndCheckCreationNamespace](../R/RangeVarGetAndCheckCreationNamespace.md)
  - [CheckSetNamespace](../C/CheckSetNamespace.md)
  - [new_object_addresses](../n/new_object_addresses.md)
  - [AlterTableNamespaceInternal](AlterTableNamespaceInternal.md)
  - [free_object_addresses](../f/free_object_addresses.md)
  - ObjectAddressSet
  - [relation_close](../r/relation_close.md)

- Called from (representative examples):
  - [ExecAlterObjectSchemaStmt](../E/ExecAlterObjectSchemaStmt.md) (main schema alteration dispatcher)

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

## Simplified Source

```c
ObjectAddress
AlterTableNamespace(AlterObjectSchemaStmt *stmt, Oid *oldschema)
{
    // Get relation OID with exclusive lock
    Oid relid = RangeVarGetRelidExtended(stmt->relation, AccessExclusiveLock,
                                        stmt->missing_ok ? RVR_MISSING_OK : 0,
                                        RangeVarCallbackForAlterRelation, stmt);

    // Handle missing relation case
    if (!OidIsValid(relid)) {
        ereport(NOTICE, "relation does not exist, skipping");
        return InvalidObjectAddress;
    }

    // Open relation and get current namespace
    Relation rel = relation_open(relid, NoLock);
    Oid oldNspOid = RelationGetNamespace(rel);

    // Special check: prevent moving owned sequences independently
    if (rel->rd_rel->relkind == RELKIND_SEQUENCE) {
        Oid tableId;
        int32 colId;
        if (sequenceIsOwned(relid, DEPENDENCY_AUTO, &tableId, &colId) ||
            sequenceIsOwned(relid, DEPENDENCY_INTERNAL, &tableId, &colId)) {
            ereport(ERROR, "cannot move an owned sequence into another schema");
        }
    }

    // Get target namespace and check permissions
    RangeVar *newrv = makeRangeVar(stmt->newschema, RelationGetRelationName(rel), -1);
    Oid nspOid = RangeVarGetAndCheckCreationNamespace(newrv, NoLock, NULL);

    // Validate namespace change is allowed
    CheckSetNamespace(oldNspOid, nspOid);

    // Perform the actual namespace change
    ObjectAddresses *objsMoved = new_object_addresses();
    AlterTableNamespaceInternal(rel, oldNspOid, nspOid, objsMoved);
    free_object_addresses(objsMoved);

    // Set up return value
    ObjectAddress myself;
    ObjectAddressSet(myself, RelationRelationId, relid);

    // Return old schema if requested
    if (oldschema)
        *oldschema = oldNspOid;

    // Keep lock until commit
    relation_close(rel, NoLock);

    return myself;
}
```
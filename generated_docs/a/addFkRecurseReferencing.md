# addFkRecurseReferencing

## Location
[src/backend/commands/tablecmds.c:10428-10603](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L10428-L10603)

## Overview
Recursively handles the referencing side of foreign key creation by creating check triggers for regular tables and recursing through partitions for partitioned tables, with support for constraint reuse and validation scheduling.

## Definition
```c
static void addFkRecurseReferencing(List **wqueue, Constraint *fkconstraint, Relation rel,
                                   Relation pkrel, Oid indexOid, Oid parentConstr,
                                   int numfks, int16 *pkattnum, int16 *fkattnum,
                                   Oid *pfeqoperators, Oid *ppeqoperators, Oid *ffeqoperators,
                                   int numfkdelsetcols, int16 *fkdelsetcols,
                                   bool old_check_ok, LOCKMODE lockmode,
                                   Oid parentInsTrigger, Oid parentUpdTrigger)
```

## Detailed Description
This function manages the referencing (foreign key) side of constraint creation with sophisticated handling for partitioned tables. It creates check triggers that enforce referential integrity and optionally schedules constraint validation for Phase 3 processing during ALTER TABLE operations.

The function operates differently based on the relation type:

For regular relations:
- Creates check triggers via createForeignKeyCheckTriggers
- Schedules constraint validation if part of an ALTER TABLE operation and validation is required

For partitioned tables:
- Recursively processes each partition with proper attribute mapping
- Attempts to reuse existing compatible foreign key constraints via tryAttachPartitionForeignKey
- Creates new constraints when reuse is not possible
- Manages trigger catalog relation efficiently during recursion

Key features include constraint reuse optimization (avoiding duplicate constraints when possible), proper attribute mapping for partitions, efficient catalog management during recursion, and integration with ALTER TABLE work queue for validation scheduling.

## Parameters / Member Variables
- `wqueue`: ALTER TABLE work queue for scheduling validation (NULL when not part of ALTER TABLE)
- `fkconstraint`: The constraint definition being processed
- `rel`: The referencing relation (foreign key table, may be a partition)
- `pkrel`: The root referenced relation (primary key table)
- `indexOid`: OID of the index implementing this constraint on pkrel
- `parentConstr`: OID of the parent constraint (always valid for this function)
- `numfks`: Number of columns in the foreign key
- `pkattnum`: Array of attribute numbers for referenced columns
- `fkattnum`: Array of attribute numbers for referencing columns
- `pfeqoperators`: Array of equality operators between PK and FK columns
- `ppeqoperators`: Array of equality operators for PK columns
- `ffeqoperators`: Array of equality operators for FK columns
- `numfkdelsetcols`: Number of columns in ON DELETE SET NULL/DEFAULT clause
- `fkdelsetcols`: Array of attribute numbers for SET action columns
- `old_check_ok`: Whether existing validation can be trusted (skip revalidation)
- `lockmode`: Lock mode to acquire on partitions during recursion
- `parentInsTrigger`: OID of parent INSERT trigger (for partition recursion)
- `parentUpdTrigger`: OID of parent UPDATE trigger (for partition recursion)

## Dependencies
- Functions called/Symbols referenced:
  - [CheckRelationLockedByMe](../C/CheckRelationLockedByMe.md)
  - [createForeignKeyCheckTriggers](../c/createForeignKeyCheckTriggers.md)
  - [ATGetQueueEntry](../A/ATGetQueueEntry.md)
  - [get_constraint_name](../g/get_constraint_name.md)
  - [RelationGetPartitionDesc](../R/RelationGetPartitionDesc.md)
  - [CheckAlterTableIsSafe](../C/CheckAlterTableIsSafe.md)
  - [build_attrmap_by_name](../b/build_attrmap_by_name.md)
  - [RelationGetFKeyList](../R/RelationGetFKeyList.md)
  - copyObject
  - [tryAttachPartitionForeignKey](../t/tryAttachPartitionForeignKey.md)
  - [addFkConstraint](addFkConstraint.md)
  - [addFkRecurseReferencing](addFkRecurseReferencing.md) (recursive call)
  - [table_open](../t/table_open.md)/table_close
- Called from (representative examples):
  - [ATAddForeignKeyConstraint](../A/ATAddForeignKeyConstraint.md)
  - [addFkRecurseReferencing](addFkRecurseReferencing.md) (recursive)
  - [CloneFkReferencing](../C/CloneFkReferencing.md)

## Notes and Other Information
- This is a static function within tablecmds.c, part of the foreign key constraint creation infrastructure
- The function is recursive and calls itself when processing partitioned tables
- Foreign tables are explicitly rejected with an error message
- [Constraint](../C/Constraint.md) reuse optimization can significantly improve performance when adding foreign keys to partitioned tables with existing compatible constraints
- The work queue integration allows proper validation scheduling during ALTER TABLE operations
- Proper lock management ensures consistency during concurrent operations
- The function efficiently manages the trigger catalog relation during partition processing to avoid excessive open/close operations
- Memory management includes proper cleanup of attribute maps and copied objects
- Phase 3 validation is only scheduled for regular relations that require it (not partitioned tables themselves)

## Simplified Source

```c
static void addFkRecurseReferencing(List **wqueue, Constraint *fkconstraint, Relation rel,
                                   Relation pkrel, Oid indexOid, Oid parentConstr,
                                   int numfks, int16 *pkattnum, int16 *fkattnum,
                                   Oid *pfeqoperators, Oid *ppeqoperators, Oid *ffeqoperators,
                                   int numfkdelsetcols, int16 *fkdelsetcols,
                                   bool old_check_ok, LOCKMODE lockmode,
                                   Oid parentInsTrigger, Oid parentUpdTrigger)
{
    Oid insertTriggerOid, updateTriggerOid;

    // Validate preconditions
    Assert(OidIsValid(parentConstr));
    Assert(CheckRelationLockedByMe(rel, ShareRowExclusiveLock, true));
    Assert(CheckRelationLockedByMe(pkrel, ShareRowExclusiveLock, true));

    // Foreign tables are not supported
    if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
        ereport(ERROR, "foreign key constraints are not supported on foreign tables");

    // Create check triggers for this relation
    createForeignKeyCheckTriggers(RelationGetRelid(rel), RelationGetRelid(pkrel),
                                 fkconstraint, parentConstr, indexOid,
                                 parentInsTrigger, parentUpdTrigger,
                                 &insertTriggerOid, &updateTriggerOid);

    if (rel->rd_rel->relkind == RELKIND_RELATION) {
        // For regular tables: schedule validation if needed
        if (wqueue && !old_check_ok && !fkconstraint->skip_validation) {
            NewConstraint *newcon = palloc0(sizeof(NewConstraint));
            AlteredTableInfo *tab = ATGetQueueEntry(wqueue, rel);

            // Set up validation task
            newcon->name = get_constraint_name(parentConstr);
            newcon->contype = CONSTR_FOREIGN;
            newcon->refrelid = RelationGetRelid(pkrel);
            newcon->refindid = indexOid;
            newcon->conid = parentConstr;
            newcon->qual = (Node *) fkconstraint;

            tab->constraints = lappend(tab->constraints, newcon);
        }
    }
    else if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) {
        // For partitioned tables: recurse to each partition
        PartitionDesc pd = RelationGetPartitionDesc(rel, true);
        Relation trigrel = table_open(TriggerRelationId, RowExclusiveLock);

        for (int i = 0; i < pd->nparts; i++) {
            Oid partitionId = pd->oids[i];
            Relation partition = table_open(partitionId, lockmode);
            AttrNumber mapped_fkattnum[INDEX_MAX_KEYS];
            bool attached = false;

            // Map attributes from parent to partition
            AttrMap *attmap = build_attrmap_by_name(RelationGetDescr(partition),
                                                   RelationGetDescr(rel), false);
            for (int j = 0; j < numfks; j++)
                mapped_fkattnum[j] = attmap->attnums[fkattnum[j] - 1];

            // Try to reuse existing compatible constraint
            List *partFKs = copyObject(RelationGetFKeyList(partition));
            foreach(cell, partFKs) {
                ForeignKeyCacheInfo *fk = lfirst_node(ForeignKeyCacheInfo, cell);
                if (tryAttachPartitionForeignKey(fk, partitionId, parentConstr,
                                               numfks, mapped_fkattnum, pkattnum,
                                               pfeqoperators, insertTriggerOid,
                                               updateTriggerOid, trigrel)) {
                    attached = true;
                    break;
                }
            }

            if (!attached) {
                // Create new constraint for this partition
                ObjectAddress address = addFkConstraint(addFkReferencingSide,
                                                      fkconstraint->conname, fkconstraint,
                                                      partition, pkrel, indexOid, parentConstr,
                                                      numfks, pkattnum, mapped_fkattnum,
                                                      pfeqoperators, ppeqoperators, ffeqoperators,
                                                      numfkdelsetcols, fkdelsetcols, true);

                // Recursively process this partition
                addFkRecurseReferencing(wqueue, fkconstraint, partition, pkrel, indexOid,
                                      address.objectId, numfks, pkattnum, mapped_fkattnum,
                                      pfeqoperators, ppeqoperators, ffeqoperators,
                                      numfkdelsetcols, fkdelsetcols, old_check_ok, lockmode,
                                      insertTriggerOid, updateTriggerOid);
            }

            table_close(partition, NoLock);
        }

        table_close(trigrel, RowExclusiveLock);
    }
}
```
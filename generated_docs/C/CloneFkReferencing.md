# CloneFkReferencing

## Location
[src/backend/commands/tablecmds.c:10830-11070](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L10830-L11070)

## Overview
CloneFkReferencing handles cloning foreign key constraints where the parent relation appears on the referencing side, attempting to reuse existing constraints before creating new ones.

## Definition

```c
structFkConstraintRow(tuple, &numfks, conkey, confkey,
								   conpfeqop, conppeqop, conffeqop,
								   &numfkdelsetcols, confdelsetcols);
```
## Detailed Description
This function is a subroutine for CloneForeignKeyConstraints that manages foreign key constraints where the parent relation is on the referencing (source) side. For each FK constraint of the parent relation, it either finds an equivalent constraint in the partition that can be reparented, or creates a new constraint as a child of the parent constraint.

The function performs several key operations:
1. Validates that the partition is not already referenced by the parent (preventing circular references)
2. Checks that foreign tables don't have FK constraints (not supported)
3. For each parent FK constraint:
   - Maps column attributes between parent and partition
   - Attempts to attach existing compatible FK constraints using tryAttachPartitionForeignKey
   - If no compatible constraint exists, creates a new FK constraint
   - Handles trigger creation and recursive processing for sub-partitions

The function includes an optimization to avoid duplicate constraints by first trying to attach existing partition constraints to the parent constraint hierarchy rather than always creating new ones.

## Parameters / Member Variables
- : Optional work queue for phase-3 verification setup (can be NULL if verification not needed)
- : The parent relation that has the foreign key constraints to be cloned
- : The partition relation where constraints will be cloned or attached

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetFKeyList](../R/RelationGetFKeyList.md): Get list of FK constraints for a relation
  - [build_attrmap_by_name](../b/build_attrmap_by_name.md): Map attributes between parent and partition
  - copyObject: Deep copy the partition's FK list
  - [DeconstructFkConstraintRow](../D/DeconstructFkConstraintRow.md): Extract FK constraint details from catalog tuple
  - [GetForeignKeyCheckTriggers](../G/GetForeignKeyCheckTriggers.md): Retrieve check trigger OIDs for constraint
  - [tryAttachPartitionForeignKey](../t/tryAttachPartitionForeignKey.md): Attempt to attach existing FK constraint to parent
  - [addFkConstraint](../a/addFkConstraint.md): Create new FK constraint on partition
  - [addFkRecurseReferencing](../a/addFkRecurseReferencing.md): Recursively handle sub-partitions
  - [find_all_inheritors](../f/find_all_inheritors.md): Lock all partitions of referenced partitioned table
  - [get_constraint_name](../g/get_constraint_name.md): Get constraint name for error reporting

- Called from:
  - [CloneForeignKeyConstraints](CloneForeignKeyConstraints.md): Main entry point for cloning FK constraints during partition operations

## Notes and Other Information
- Includes protection against circular FK relationships by preventing attachment of tables that the parent already references
- Foreign tables cannot have FK constraints and will cause an error if attempted
- Uses an optimization strategy: tries to reuse existing compatible constraints before creating new ones
- Requires careful locking of referenced relations, especially when they are partitioned tables
- The wqueue parameter enables deferred constraint validation during ATTACH PARTITION operations
- Part of PostgreSQL's comprehensive partition-wise foreign key constraint management system

## Simplified Source

```c
static void
CloneFkReferencing(List **wqueue, Relation parentRel, Relation partRel)
{
    AttrMap *attmap;
    List *partFKs;
    List *clone = NIL;
    ListCell *cell;
    Relation trigrel;

    // Build list of FK constraints to clone from parent
    foreach(cell, RelationGetFKeyList(parentRel))
    {
        ForeignKeyCacheInfo *fk = lfirst(cell);

        // Prevent circular references: parent can't reference partition
        if (fk->confrelid == RelationGetRelid(partRel))
            ereport(ERROR, "cannot attach partition referenced by FK");

        clone = lappend_oid(clone, fk->conoid);
    }

    if (clone == NIL)
        return;

    // Foreign tables cannot have FK constraints
    if (partRel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
        ereport(ERROR, "FK constraints not supported on foreign tables");

    // Open trigger catalog for manipulation
    trigrel = table_open(TriggerRelationId, RowExclusiveLock);

    // Build attribute mapping between parent and partition
    attmap = build_attrmap_by_name(RelationGetDescr(partRel),
                                   RelationGetDescr(parentRel), false);

    partFKs = copyObject(RelationGetFKeyList(partRel));

    // Process each FK constraint to clone
    foreach(cell, clone)
    {
        Oid parentConstrOid = lfirst_oid(cell);
        HeapTuple tuple;
        Form_pg_constraint constrForm;
        Relation pkrel;
        bool attached = false;

        // Get constraint details from catalog
        tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(parentConstrOid));
        constrForm = (Form_pg_constraint) GETSTRUCT(tuple);

        // Skip if parent constraint is also being cloned
        if (list_member_oid(clone, constrForm->conparentid))
        {
            ReleaseSysCache(tuple);
            continue;
        }

        // Lock referenced table and all its partitions
        pkrel = table_open(constrForm->confrelid, ShareRowExclusiveLock);
        if (pkrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
            find_all_inheritors(RelationGetRelid(pkrel), ShareRowExclusiveLock, NULL);

        // Extract constraint components and map column numbers
        int numfks;
        AttrNumber conkey[INDEX_MAX_KEYS];
        AttrNumber mapped_conkey[INDEX_MAX_KEYS];
        AttrNumber confkey[INDEX_MAX_KEYS];
        // ... other constraint details extracted

        DeconstructFkConstraintRow(tuple, &numfks, conkey, confkey, /*...*/);

        // Map parent column numbers to partition column numbers
        for (int i = 0; i < numfks; i++)
            mapped_conkey[i] = attmap->attnums[conkey[i] - 1];

        // Try to attach existing compatible FK constraint
        foreach(lc, partFKs)
        {
            ForeignKeyCacheInfo *fk = lfirst_node(ForeignKeyCacheInfo, lc);

            if (tryAttachPartitionForeignKey(fk, RelationGetRelid(partRel),
                                           parentConstrOid, numfks, mapped_conkey,
                                           confkey, /*...*/, trigrel))
            {
                attached = true;
                break;
            }
        }

        if (attached)
        {
            ReleaseSysCache(tuple);
            table_close(pkrel, NoLock);
            continue;
        }

        // Create new FK constraint since no existing one could be attached
        Constraint *fkconstraint = makeNode(Constraint);
        fkconstraint->contype = CONSTRAINT_FOREIGN;
        // ... set up constraint properties from parent constraint

        // Create constraint entry and triggers
        ObjectAddress address = addFkConstraint(/*...*/);
        addFkRecurseReferencing(wqueue, fkconstraint, partRel, pkrel, /*...*/);

        ReleaseSysCache(tuple);
        table_close(pkrel, NoLock);
    }

    table_close(trigrel, RowExclusiveLock);
}
```
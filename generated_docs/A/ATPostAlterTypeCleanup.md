# ATPostAlterTypeCleanup

## Location
[src/backend/commands/tablecmds.c:13840-14030](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L13840-L14030)

## Overview
ATPostAlterTypeCleanup handles the cleanup phase after ALTER TYPE or SET EXPRESSION operations, dropping and scheduling recreation of all dependent indexes, constraints, and statistics objects.

## Definition
```c
static void ATPostAlterTypeCleanup(List **wqueue, AlteredTableInfo *tab, LOCKMODE lockmode)
```

## Detailed Description
This function performs the critical cleanup phase after column type alterations are complete. It systematically processes all indexes, constraints, and statistics objects that were marked for rebuilding during the type change operation. The function operates in two main phases: first, it re-parses all dependent object definitions and queues their recreation in the work queue; second, it drops all the old objects in a single batch operation. The function handles complex scenarios like foreign key constraints on other tables, inheritance hierarchies, and cross-table dependencies while managing appropriate locking. It also restores special table properties like replica identity and clustering after the objects are recreated.

## Parameters / Member Variables
- `wqueue`: Double pointer to the ALTER TABLE work queue where recreation commands are added
- `tab`: Pointer to AlteredTableInfo structure containing lists of objects to rebuild
- `lockmode`: Lock mode to use for the operations

## Dependencies
- Functions called/Symbols referenced:
  - [new_object_addresses](../n/new_object_addresses.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [get_typ_typrelid](../g/get_typ_typrelid.md)
  - [getBaseType](../g/getBaseType.md)
  - ObjectAddressSet
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - [LockRelationOid](../L/LockRelationOid.md)
  - [ATPostAlterTypeParse](ATPostAlterTypeParse.md)
  - [IndexGetRelation](../I/IndexGetRelation.md)
  - [StatisticsGetRelation](../S/StatisticsGetRelation.md)
  - makeNode
  - [performMultipleDeletions](../p/performMultipleDeletions.md)
  - [free_object_addresses](../f/free_object_addresses.md)
  - [AlteredTableInfo](AlteredTableInfo.md) (struct)
  - ObjectAddresses (struct)
  - Form_pg_constraint (struct)
- Called from (representative examples):
  - [ATRewriteCatalogs](ATRewriteCatalogs.md)
  - child_dependency_type

## Notes and Other Information
- Processes constraints, indexes, and statistics in separate loops with different locking strategies
- Uses AccessExclusiveLock for constraints and indexes, ShareUpdateExclusiveLock for statistics
- Handles inherited constraints by skipping recreation for non-local constraints
- Queues replica identity and cluster property restoration commands for later execution
- Uses DROP_RESTRICT for safety since dependencies should already be handled
- Critical for maintaining database consistency during complex type change operations

## Simplified Source

```c
static void
ATPostAlterTypeCleanup(List **wqueue, AlteredTableInfo *tab, LOCKMODE lockmode)
{
    ObjectAddress obj;
    ObjectAddresses *objects;
    ListCell *def_item;
    ListCell *oid_item;

    // Collect all objects to drop in a single batch
    objects = new_object_addresses();

    // Process constraints marked for rebuild
    forboth(oid_item, tab->changedConstraintOids,
            def_item, tab->changedConstraintDefs)
    {
        Oid oldId = lfirst_oid(oid_item);
        HeapTuple tup;
        Form_pg_constraint con;
        Oid relid, confrelid;
        bool conislocal;

        // Look up constraint details
        tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(oldId));
        con = (Form_pg_constraint) GETSTRUCT(tup);

        if (OidIsValid(con->conrelid))
            relid = con->conrelid;
        else
            relid = get_typ_typrelid(getBaseType(con->contypid)); // domain constraint

        confrelid = con->confrelid;
        conislocal = con->conislocal;
        ReleaseSysCache(tup);

        // Add to deletion list
        ObjectAddressSet(obj, ConstraintRelationId, oldId);
        add_exact_object_address(&obj, objects);

        // Skip inherited-only constraints
        if (!conislocal)
            continue;

        // Lock other table if needed for cross-table constraints
        if (relid != tab->relid)
            LockRelationOid(relid, AccessExclusiveLock);

        // Schedule constraint recreation
        ATPostAlterTypeParse(oldId, relid, confrelid,
                             (char *) lfirst(def_item),
                             wqueue, lockmode, tab->rewrite);
    }

    // Process indexes marked for rebuild
    forboth(oid_item, tab->changedIndexOids,
            def_item, tab->changedIndexDefs)
    {
        Oid oldId = lfirst_oid(oid_item);
        Oid relid = IndexGetRelation(oldId, false);

        // Lock other table if needed
        if (relid != tab->relid)
            LockRelationOid(relid, AccessExclusiveLock);

        // Schedule index recreation
        ATPostAlterTypeParse(oldId, relid, InvalidOid,
                             (char *) lfirst(def_item),
                             wqueue, lockmode, tab->rewrite);

        ObjectAddressSet(obj, RelationRelationId, oldId);
        add_exact_object_address(&obj, objects);
    }

    // Process statistics objects similarly
    forboth(oid_item, tab->changedStatisticsOids,
            def_item, tab->changedStatisticsDefs)
    {
        Oid oldId = lfirst_oid(oid_item);
        Oid relid = StatisticsGetRelation(oldId, false);

        if (relid != tab->relid)
            LockRelationOid(relid, ShareUpdateExclusiveLock);

        ATPostAlterTypeParse(oldId, relid, InvalidOid,
                             (char *) lfirst(def_item),
                             wqueue, lockmode, tab->rewrite);

        ObjectAddressSet(obj, StatisticExtRelationId, oldId);
        add_exact_object_address(&obj, objects);
    }

    // Queue restoration of replica identity index
    if (tab->replicaIdentityIndex)
    {
        AlterTableCmd *cmd = makeNode(AlterTableCmd);
        ReplicaIdentityStmt *subcmd = makeNode(ReplicaIdentityStmt);

        subcmd->identity_type = REPLICA_IDENTITY_INDEX;
        subcmd->name = tab->replicaIdentityIndex;
        cmd->subtype = AT_ReplicaIdentity;
        cmd->def = (Node *) subcmd;

        tab->subcmds[AT_PASS_OLD_CONSTR] =
            lappend(tab->subcmds[AT_PASS_OLD_CONSTR], cmd);
    }

    // Queue restoration of cluster index
    if (tab->clusterOnIndex)
    {
        AlterTableCmd *cmd = makeNode(AlterTableCmd);
        cmd->subtype = AT_ClusterOn;
        cmd->name = tab->clusterOnIndex;

        tab->subcmds[AT_PASS_OLD_CONSTR] =
            lappend(tab->subcmds[AT_PASS_OLD_CONSTR], cmd);
    }

    // Drop all old objects in one batch
    performMultipleDeletions(objects, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
    free_object_addresses(objects);
}
```
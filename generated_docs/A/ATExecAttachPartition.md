# ATExecAttachPartition

## Location
[src/backend/commands/tablecmds.c:18487-18802](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L18487-L18802)

## Overview
ATExecAttachPartition implements the ALTER TABLE ATTACH PARTITION command, performing comprehensive validation and setup to attach a new table as a partition to a partitioned table.

## Definition
```c
static ObjectAddress ATExecAttachPartition(List **wqueue, Relation rel, PartitionCmd *cmd,
                                           AlterTableUtilityContext *context)
```

## Detailed Description
This function handles the complex process of attaching a table as a partition to a partitioned table. The operation involves extensive validation, constraint setup, and metadata updates:

**Validation Phase:**
- Checks permissions on both parent and child tables
- Validates that the table is not already a partition
- Prevents circular inheritance relationships
- Ensures compatible persistence levels (temporary/permanent)
- Validates column compatibility between parent and child
- Checks for incompatible features (identity columns, certain triggers)

**Setup Phase:**
- Establishes inheritance relationship via CreateInheritance
- Updates partition boundary information in pg_class
- Ensures matching indexes exist on the partition
- Clones row triggers and foreign key constraints
- Generates and validates partition constraints

**Constraint Management:**
- Creates partition boundary constraints from the FOR VALUES specification
- Combines with parent partition quals if the parent is itself a partition
- Queues constraint validation work for the new partition
- Updates default partition constraints if a default partition exists

The function integrates with PostgreSQL's three-phase ALTER TABLE model by queuing validation work for Phase 3 execution.

## Parameters / Member Variables
- `wqueue`: Work queue for storing validation tasks to be executed in Phase 3
- `rel`: The parent partitioned table relation
- `cmd`: PartitionCmd containing the partition boundary specification and table name
- `context`: ALTER TABLE context containing query string and other metadata

## Dependencies
- Functions called/Symbols referenced:
  - [make_parsestate](../m/make_parsestate.md), get_default_oid_from_partdesc, LockRelationOid
  - [table_openrv](../t/table_openrv.md), ATSimplePermissions, find_all_inheritors
  - [check_new_partition_bound](../c/check_new_partition_bound.md), CreateInheritance, StorePartitionBound
  - [AttachPartitionEnsureIndexes](AttachPartitionEnsureIndexes.md), CloneRowTriggersToPartition, CloneForeignKeyConstraints
  - [get_qual_from_partbound](../g/get_qual_from_partbound.md), RelationGetPartitionQual, QueuePartitionConstraintValidation
  - [get_proposed_default_constraint](../g/get_proposed_default_constraint.md), map_partition_varattnos
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md)
  - child_dependency_type

## Notes and Other Information
- Static function used internally within ALTER TABLE processing
- Uses AccessExclusiveLock throughout to prevent concurrent modifications
- Handles both regular and partitioned tables as attachments
- Updates default partition constraints when attaching non-default partitions
- Invalidates relcache for descendent partitions when attaching partitioned tables
- Prevents attachment of tables with identity columns or incompatible triggers
- Maintains locks until transaction commit for consistency
- Returns ObjectAddress pointing to the newly attached partition
- Part of PostgreSQL's comprehensive partitioning infrastructure

## Simplified Source

```c
static ObjectAddress
ATExecAttachPartition(List **wqueue, Relation rel, PartitionCmd *cmd,
                     AlterTableUtilityContext *context)
{
    Relation attachrel, catalog;
    List *attachrel_children;
    List *partConstraint;
    AttrNumber attno;
    TupleDesc tupleDesc;
    ObjectAddress address;
    const char *trigger_name;
    Oid defaultPartOid;
    List *partBoundConstraint;
    ParseState *pstate = make_parsestate(NULL);

    pstate->p_sourcetext = context->queryString;

    // Lock default partition if it exists (constraint will change)
    defaultPartOid = get_default_oid_from_partdesc(RelationGetPartitionDesc(rel, true));
    if (OidIsValid(defaultPartOid))
        LockRelationOid(defaultPartOid, AccessExclusiveLock);

    // Open and validate the table to be attached
    attachrel = table_openrv(cmd->name, AccessExclusiveLock);
    ATSimplePermissions(AT_AttachPartition, attachrel, ATT_TABLE | ATT_FOREIGN_TABLE);

    // Validate table is not already a partition or typed table
    if (attachrel->rd_rel->relispartition)
        ereport(ERROR, "\"%s\" is already a partition", RelationGetRelationName(attachrel));

    if (OidIsValid(attachrel->rd_rel->reloftype))
        ereport(ERROR, "cannot attach a typed table as partition");

    // Check inheritance relationships - table should not be in inheritance hierarchy
    catalog = table_open(InheritsRelationId, AccessShareLock);
    ScanKeyData skey;
    ScanKeyInit(&skey, Anum_pg_inherits_inhrelid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(attachrel)));
    SysScanDesc scan = systable_beginscan(catalog, InheritsRelidSeqnoIndexId, true, NULL, 1, &skey);
    if (HeapTupleIsValid(systable_getnext(scan)))
        ereport(ERROR, "cannot attach inheritance child as partition");
    systable_endscan(scan);

    // Check if table is an inheritance parent (except if partitioned)
    ScanKeyInit(&skey, Anum_pg_inherits_inhparent, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(attachrel)));
    scan = systable_beginscan(catalog, InheritsParentIndexId, true, NULL, 1, &skey);
    if (HeapTupleIsValid(systable_getnext(scan)) &&
        attachrel->rd_rel->relkind == RELKIND_RELATION)
        ereport(ERROR, "cannot attach inheritance parent as partition");
    systable_endscan(scan);
    table_close(catalog, AccessShareLock);

    // Prevent circular inheritance
    attachrel_children = find_all_inheritors(RelationGetRelid(attachrel), AccessExclusiveLock, NULL);
    if (list_member_oid(attachrel_children, RelationGetRelid(rel)))
        ereport(ERROR, "circular inheritance not allowed");

    // Validate persistence compatibility (temp/permanent)
    if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP &&
        attachrel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
        ereport(ERROR, "cannot attach temporary relation as partition of permanent relation");

    if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP &&
        attachrel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
        ereport(ERROR, "cannot attach permanent relation as partition of temporary relation");

    // Check temp table session ownership
    if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP && !rel->rd_islocaltemp)
        ereport(ERROR, "cannot attach as partition of temporary relation of another session");

    if (attachrel->rd_rel->relpersistence == RELPERSISTENCE_TEMP && !attachrel->rd_islocaltemp)
        ereport(ERROR, "cannot attach temporary relation of another session as partition");

    // Validate column compatibility
    tupleDesc = RelationGetDescr(attachrel);
    for (attno = 1; attno <= tupleDesc->natts; attno++) {
        Form_pg_attribute attribute = TupleDescAttr(tupleDesc, attno - 1);
        char *attributeName = NameStr(attribute->attname);

        if (attribute->attisdropped)
            continue;

        // Reject identity columns
        if (attribute->attidentity)
            ereport(ERROR, "table being attached contains an identity column \"%s\"",
                    attributeName);

        // Ensure column exists in parent
        if (!SearchSysCacheExists2(ATTNAME, ObjectIdGetDatum(RelationGetRelid(rel)),
                                  CStringGetDatum(attributeName)))
            ereport(ERROR, "table \"%s\" contains column \"%s\" not found in parent",
                    RelationGetRelationName(attachrel), attributeName);
    }

    // Check for incompatible triggers
    trigger_name = FindTriggerIncompatibleWithInheritance(attachrel->trigdesc);
    if (trigger_name != NULL)
        ereport(ERROR, "trigger \"%s\" prevents table from becoming a partition",
                trigger_name);

    // Validate partition bound doesn't overlap existing partitions
    check_new_partition_bound(RelationGetRelationName(attachrel), rel, cmd->bound, pstate);

    // Create inheritance relationship and update catalogs
    CreateInheritance(attachrel, rel, true);
    StorePartitionBound(attachrel, rel, cmd->bound);

    // Set up indexes, triggers, and foreign keys
    AttachPartitionEnsureIndexes(wqueue, rel, attachrel);
    CloneRowTriggersToPartition(rel, attachrel);
    CloneForeignKeyConstraints(wqueue, rel, attachrel);

    // Generate and validate partition constraints
    partBoundConstraint = get_qual_from_partbound(rel, cmd->bound);
    partConstraint = list_concat(partBoundConstraint, RelationGetPartitionQual(rel));

    if (partConstraint) {
        // Simplify and adjust constraints for this partition
        partConstraint = (List *) eval_const_expressions(NULL, (Node *) partConstraint);
        partConstraint = list_make1(make_ands_explicit(partConstraint));
        partConstraint = map_partition_varattnos(partConstraint, 1, attachrel, rel);

        // Queue constraint validation for Phase 3
        QueuePartitionConstraintValidation(wqueue, attachrel, partConstraint, false);
    }

    // Update default partition constraint if needed
    if (OidIsValid(defaultPartOid)) {
        Relation defaultrel = table_open(defaultPartOid, NoLock);
        List *defPartConstraint = get_proposed_default_constraint(partBoundConstraint);
        defPartConstraint = map_partition_varattnos(defPartConstraint, 1, defaultrel, rel);
        QueuePartitionConstraintValidation(wqueue, defaultrel, defPartConstraint, true);
        table_close(defaultrel, NoLock);
    }

    ObjectAddressSet(address, RelationRelationId, RelationGetRelid(attachrel));

    // Invalidate relcache for partitioned table descendants
    if (attachrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) {
        ListCell *l;
        foreach(l, attachrel_children)
            CacheInvalidateRelcacheByRelid(lfirst_oid(l));
    }

    table_close(attachrel, NoLock);
    return address;
}
```
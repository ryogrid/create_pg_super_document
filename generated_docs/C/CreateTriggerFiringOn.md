# CreateTriggerFiringOn

## Location
[src/backend/commands/trigger.c:176-1215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L176-L1215)

## Overview
CreateTriggerFiringOn is the core PostgreSQL function that implements trigger creation with support for custom firing conditions, handling all the complex validation, catalog operations, and partition recursion.

## Definition
```c
ObjectAddress CreateTriggerFiringOn(CreateTrigStmt *stmt, const char *queryString,
                                   Oid relOid, Oid refRelOid, Oid constraintOid,
                                   Oid indexOid, Oid funcoid, Oid parentTriggerOid,
                                   Node *whenClause, bool isInternal, bool in_partition,
                                   char trigger_fires_when)
```

## Detailed Description
CreateTriggerFiringOn is the comprehensive implementation of trigger creation in PostgreSQL. It performs extensive validation of trigger properties against relation types, handles permission checks, validates the trigger function, processes WHEN clauses, manages transition tables, and creates the pg_trigger catalog entry. The function supports all trigger types (BEFORE/AFTER/INSTEAD OF) across different relation kinds (tables, views, foreign tables, partitioned tables) and automatically recurses to create triggers on partitions when appropriate. It also handles trigger replacement with OR REPLACE semantics and establishes proper dependency relationships.

## Parameters / Member Variables
- `stmt`: CreateTrigStmt structure with parsed CREATE TRIGGER statement details
- `queryString`: Source text of CREATE TRIGGER command (needed for WHEN clause parsing)
- `relOid`: Target relation OID (0 to look up by name from stmt->relation)
- `refRelOid`: Constraint reference relation OID (for constraint triggers)
- `constraintOid`: Associated constraint OID (0 for non-constraint triggers)
- `indexOid`: Associated index OID (stored in tgconstrindid field)
- `funcoid`: Trigger function OID (0 to look up from stmt->funcname)
- `parentTriggerOid`: Parent trigger OID for inheritance/partition relationships
- `whenClause`: Pre-transformed WHEN condition (overrides stmt->whenClause)
- `isInternal`: Whether this is an internally-generated trigger
- `in_partition`: Indicates recursive call for partition trigger creation
- `trigger_fires_when`: Firing condition (ORIGIN/ALWAYS/REPLICA/DISABLED)

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)/table_openrv
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md)
  - [has_superclass](../h/has_superclass.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [CreateConstraintEntry](CreateConstraintEntry.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [deleteDependencyRecordsFor](../d/deleteDependencyRecordsFor.md)
  - [map_partition_varattnos](../m/map_partition_varattnos.md)
- Called from (representative examples):
  - [CreateTrigger](CreateTrigger.md)
  - [CloneRowTriggersToPartition](CloneRowTriggersToPartition.md)

## Notes and Other Information
- Performs comprehensive relation type validation (tables, views, foreign tables, partitioned tables)
- Handles complex WHEN clause parsing with OLD/NEW variable validation
- Supports transition table validation with extensive restrictions
- Manages trigger name uniqueness for internal triggers by appending OID
- Automatically recurses to partitions for row-level triggers on partitioned tables
- Implements OR REPLACE semantics with proper validation of existing triggers
- Creates proper dependency relationships for functions, constraints, and parent triggers
- Validates trigger function return type must be 'trigger'
- Enforces security with ACL_TRIGGER and ACL_EXECUTE permission checks

## Simplified Source

```c
ObjectAddress
CreateTriggerFiringOn(CreateTrigStmt *stmt, const char *queryString,
                      Oid relOid, Oid refRelOid, Oid constraintOid,
                      Oid indexOid, Oid funcoid, Oid parentTriggerOid,
                      Node *whenClause, bool isInternal, bool in_partition,
                      char trigger_fires_when)
{
    int16 tgtype;
    Relation rel;
    HeapTuple tuple;
    Oid trigoid;
    ObjectAddress myself;

    // Open target relation with proper lock
    if (OidIsValid(relOid))
        rel = table_open(relOid, ShareRowExclusiveLock);
    else
        rel = table_openrv(stmt->relation, ShareRowExclusiveLock);

    // Validate relation type compatibility with trigger type
    switch (rel->rd_rel->relkind)
    {
        case RELKIND_RELATION:
        case RELKIND_PARTITIONED_TABLE:
            // Tables can't have INSTEAD OF triggers
            if (stmt->timing != TRIGGER_TYPE_BEFORE && stmt->timing != TRIGGER_TYPE_AFTER)
                ereport(ERROR, "Tables cannot have INSTEAD OF triggers");
            break;

        case RELKIND_VIEW:
            // Views have specific trigger restrictions
            if (stmt->timing != TRIGGER_TYPE_INSTEAD && stmt->row)
                ereport(ERROR, "Views cannot have row-level BEFORE/AFTER triggers");
            if (TRIGGER_FOR_TRUNCATE(stmt->events))
                ereport(ERROR, "Views cannot have TRUNCATE triggers");
            break;

        case RELKIND_FOREIGN_TABLE:
            // Foreign tables have limited trigger support
            if (stmt->timing != TRIGGER_TYPE_BEFORE && stmt->timing != TRIGGER_TYPE_AFTER)
                ereport(ERROR, "Foreign tables cannot have INSTEAD OF triggers");
            if (stmt->isconstraint)
                ereport(ERROR, "Foreign tables cannot have constraint triggers");
            break;

        default:
            ereport(ERROR, "Relation cannot have triggers");
    }

    // Check permissions unless internal trigger
    if (!isInternal)
    {
        // Check ACL_TRIGGER permission on target relation
        // Check ACL_TRIGGER permission on constraint relation if applicable
        // Check ACL_EXECUTE permission on trigger function
    }

    // Set up partitioning recursion if needed
    bool partition_recurse = !isInternal && stmt->row &&
        rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE;
    if (partition_recurse)
        find_all_inheritors(RelationGetRelid(rel), ShareRowExclusiveLock, NULL);

    // Build trigger type flags
    TRIGGER_CLEAR_TYPE(tgtype);
    if (stmt->row)
        TRIGGER_SETT_ROW(tgtype);
    tgtype |= stmt->timing;
    tgtype |= stmt->events;

    // Validate trigger type combinations
    if (TRIGGER_FOR_ROW(tgtype) && TRIGGER_FOR_TRUNCATE(tgtype))
        ereport(ERROR, "TRUNCATE FOR EACH ROW triggers not supported");

    if (TRIGGER_FOR_INSTEAD(tgtype))
    {
        if (!TRIGGER_FOR_ROW(tgtype))
            ereport(ERROR, "INSTEAD OF triggers must be FOR EACH ROW");
        // Additional INSTEAD OF restrictions...
    }

    // Process transition tables if specified
    if (stmt->transitionRels != NIL)
    {
        // Validate transition table specifications
        // Check compatibility with relation type and trigger type
        // Set up old/new table names
    }

    // Parse WHEN clause if provided
    if (stmt->whenClause && !whenClause)
    {
        // Set up parse state with OLD/NEW references
        // Transform WHEN expression
        // Validate OLD/NEW variable usage
    }

    // Validate trigger function
    if (!OidIsValid(funcoid))
        funcoid = LookupFuncName(stmt->funcname, 0, NULL, false);

    if (get_func_rettype(funcoid) != TRIGGEROID)
        ereport(ERROR, "Function must return type trigger");

    // Check for existing trigger and handle OR REPLACE
    Relation tgrel = table_open(TriggerRelationId, RowExclusiveLock);
    bool trigger_exists = false;

    if (!isInternal)
    {
        // Scan for existing trigger with same name
        // Handle OR REPLACE logic
    }

    if (!trigger_exists)
        trigoid = GetNewOidWithIndex(tgrel, TriggerOidIndexId, Anum_pg_trigger_oid);

    // Create constraint entry if needed
    if (stmt->isconstraint && !OidIsValid(constraintOid))
    {
        constraintOid = CreateConstraintEntry(/* constraint details */);
    }

    // Generate internal trigger name if needed
    char internaltrigname[NAMEDATALEN];
    char *trigname;
    if (isInternal)
    {
        snprintf(internaltrigname, sizeof(internaltrigname),
                 "%s_%u", stmt->trigname, trigoid);
        trigname = internaltrigname;
    }
    else
        trigname = stmt->trigname;

    // Build and insert pg_trigger tuple
    Datum values[Natts_pg_trigger];
    bool nulls[Natts_pg_trigger];

    // Set all trigger attributes...
    values[Anum_pg_trigger_tgname - 1] = CStringGetDatum(trigname);
    values[Anum_pg_trigger_tgfoid - 1] = ObjectIdGetDatum(funcoid);
    values[Anum_pg_trigger_tgtype - 1] = Int16GetDatum(tgtype);
    // ... other attributes

    if (!trigger_exists)
    {
        tuple = heap_form_tuple(tgrel->rd_att, values, nulls);
        CatalogTupleInsert(tgrel, tuple);
    }
    else
    {
        // Update existing trigger
        HeapTuple newtup = heap_form_tuple(tgrel->rd_att, values, nulls);
        CatalogTupleUpdate(tgrel, &tuple->t_self, newtup);
        heap_freetuple(newtup);
    }

    table_close(tgrel, RowExclusiveLock);

    // Update relation's relhastriggers flag if needed
    // Record dependencies
    // Create triggers on partitions if needed

    myself.classId = TriggerRelationId;
    myself.objectId = trigoid;
    myself.objectSubId = 0;

    table_close(rel, NoLock);
    return myself;
}
```
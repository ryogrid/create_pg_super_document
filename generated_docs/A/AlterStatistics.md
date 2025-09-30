# AlterStatistics

## Location
[src/backend/commands/statscmds.c:599-721](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/statscmds.c#L599-L721)

## Overview
Modifies the statistics target of an existing PostgreSQL extended statistics object, controlling how much sample data is collected during ANALYZE operations.

## Definition

```c
structQualifiedName(stmt->defnames, &schemaname, &statname);
```
## Detailed Description
This function implements the ALTER STATISTICS SQL command, specifically handling changes to the statistics target parameter. The statistics target determines the sample size used when collecting extended statistics during ANALYZE operations - higher values provide more accurate statistics but require more storage and computation time.

The function validates the new target value (must be between 0 and MAX_STATISTICS_TARGET), checks object ownership permissions, and updates the stxstattarget column in the pg_statistic_ext system catalog. It supports the IF EXISTS clause to gracefully handle non-existent statistics objects.

Key validation steps include:
- Range checking the statistics target value
- Verifying object existence and ownership  
- Proper handling of default values (NULL in catalog)
- Warning when target exceeds maximum and auto-clamping to MAX_STATISTICS_TARGET

## Parameters / Member Variables
- : AlterStatsStmt structure containing the statistics object name, new target value, and IF EXISTS flag from the parsed ALTER STATISTICS command

## Dependencies
- Functions called/Symbols referenced:
  - [get_statistics_object_oid](../g/get_statistics_object_oid.md) (resolves statistics object name to OID)
  - [object_ownercheck](../o/object_ownercheck.md) (verifies ownership permissions)  
  - [heap_modify_tuple](../h/heap_modify_tuple.md) (creates updated catalog tuple)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (commits changes to pg_statistic_ext)
  - InvokeObjectPostAlterHook (triggers post-alter event hooks)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1907)

## Notes and Other Information
- Only supports altering the statistics target, not other statistics object properties
- Target value of -1 (from previous PostgreSQL versions) is treated as default
- Default target uses NULL value in catalog (inherits from default_statistics_target GUC)
- Values above MAX_STATISTICS_TARGET are automatically clamped with a warning
- No dependency updates needed since only the target value changes
- Returns InvalidObjectAddress when IF EXISTS is used and object doesn't exist
- Requires RowExclusiveLock on StatisticExtRelationId catalog table

## Simplified Source

```c
ObjectAddress
AlterStatistics(AlterStatsStmt *stmt)
{
    Relation rel;
    Oid stxoid;
    HeapTuple oldtup, newtup;
    Datum repl_val[Natts_pg_statistic_ext];
    bool repl_null[Natts_pg_statistic_ext];
    bool repl_repl[Natts_pg_statistic_ext];
    ObjectAddress address;
    int newtarget = 0;
    bool newtarget_default;

    // Parse target value (-1 means default in older versions)
    if (stmt->stxstattarget && intVal(stmt->stxstattarget) != -1) {
        newtarget = intVal(stmt->stxstattarget);
        newtarget_default = false;
    } else {
        newtarget_default = true;
    }

    // Validate target range if not using default
    if (!newtarget_default) {
        if (newtarget < 0) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("statistics target %d is too low", newtarget)));
        } else if (newtarget > MAX_STATISTICS_TARGET) {
            newtarget = MAX_STATISTICS_TARGET;
            ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                             errmsg("lowering statistics target to %d", newtarget)));
        }
    }

    // Look up statistics object
    stxoid = get_statistics_object_oid(stmt->defnames, stmt->missing_ok);

    // Handle IF EXISTS case when object doesn't exist
    if (!OidIsValid(stxoid)) {
        char *schemaname, *statname;
        Assert(stmt->missing_ok);

        DeconstructQualifiedName(stmt->defnames, &schemaname, &statname);
        if (schemaname)
            ereport(NOTICE, (errmsg("statistics object \"%s.%s\" does not exist, skipping",
                                   schemaname, statname)));
        else
            ereport(NOTICE, (errmsg("statistics object \"%s\" does not exist, skipping",
                                   statname)));

        return InvalidObjectAddress;
    }

    // Open catalog and find the statistics object
    rel = table_open(StatisticExtRelationId, RowExclusiveLock);
    oldtup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(stxoid));
    if (!HeapTupleIsValid(oldtup))
        elog(ERROR, "cache lookup failed for extended statistics object %u", stxoid);

    // Check ownership permissions
    if (!object_ownercheck(StatisticExtRelationId, stxoid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_STATISTIC_EXT,
                      NameListToString(stmt->defnames));

    // Build updated tuple
    memset(repl_val, 0, sizeof(repl_val));
    memset(repl_null, false, sizeof(repl_null));
    memset(repl_repl, false, sizeof(repl_repl));

    // Set new statistics target value
    repl_repl[Anum_pg_statistic_ext_stxstattarget - 1] = true;
    if (!newtarget_default)
        repl_val[Anum_pg_statistic_ext_stxstattarget - 1] = Int16GetDatum(newtarget);
    else
        repl_null[Anum_pg_statistic_ext_stxstattarget - 1] = true;

    newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel),
                              repl_val, repl_null, repl_repl);

    // Update catalog
    CatalogTupleUpdate(rel, &newtup->t_self, newtup);

    InvokeObjectPostAlterHook(StatisticExtRelationId, stxoid, 0);
    ObjectAddressSet(address, StatisticExtRelationId, stxoid);

    // Cleanup
    heap_freetuple(newtup);
    ReleaseSysCache(oldtup);
    table_close(rel, RowExclusiveLock);

    return address;
}
```
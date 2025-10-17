# RI_FKey_cascade_del

## Location
[src/backend/utils/adt/ri_triggers.c:743-848](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L743-L848)

## Overview
A trigger function that implements CASCADE behavior for DELETE operations, automatically deleting all rows in foreign key tables that reference the deleted primary key row.

## Definition
```c
Datum RI_FKey_cascade_del(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL trigger function that enforces the CASCADE referential integrity constraint when rows are deleted from a referenced (parent) table. When a CASCADE constraint is defined, deleting a row from the primary key table should automatically delete all rows in foreign key tables that reference the deleted row, maintaining referential integrity by eliminating orphaned references.

The function builds and executes a DELETE statement against the foreign key table to remove all rows that reference the deleted primary key values. The query constructed is of the form: `DELETE FROM [ONLY] <fktable> WHERE  = fkatt1 [AND ...]`, using the primary key values from the deleted row as parameters.

The function uses RowExclusiveLock on the foreign key relation since it will perform DELETE operations, and uses the SPI interface to execute the cascaded delete query.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `fcinfo`: Function call information structure containing trigger data and context

## Dependencies
- Functions called/Symbols referenced:
  - [ri_CheckTrigger](../r/ri_CheckTrigger.md): Validates the trigger call context
  - [ri_FetchConstraintInfo](../r/ri_FetchConstraintInfo.md): Retrieves constraint metadata
  - `[table_open](../t/table_open.md)`: Opens the foreign key relation with RowExclusiveLock
  - [ri_BuildQueryKey](../r/ri_BuildQueryKey.md): Builds query cache key
  - [ri_FetchPreparedPlan](../r/ri_FetchPreparedPlan.md): Retrieves cached query plan
  - [ri_GenerateQual](../r/ri_GenerateQual.md): Generates WHERE clause conditions
  - [ri_PlanCheck](../r/ri_PlanCheck.md): Prepares and caches the DELETE query plan
  - [ri_PerformCheck](../r/ri_PerformCheck.md): Executes the cascaded delete operation
  - SPI functions: `SPI_connect`, `SPI_finish`
  - Various utility functions for name quoting and type handling
  - `RI_PLAN_CASCADE_ONDELETE`: Query plan type constant
  - `RI_TRIGTYPE_DELETE`: DELETE trigger type constant

- Called from (representative examples):
  - No direct callers found (invoked by PostgreSQL trigger system)

## Notes and Other Information
- This function is registered as a trigger function in the PostgreSQL system catalog
- Uses RowExclusiveLock mode on the foreign key relation since DELETE operations will be performed
- Implements query plan caching for performance optimization using `RI_PLAN_CASCADE_ONDELETE`
- Handles partitioned tables by omitting ONLY keyword when appropriate
- The cascaded deletes can trigger additional cascades if the foreign key tables have their own CASCADE constraints
- Part of PostgreSQL's comprehensive referential integrity system
- Located in `src/backend/utils/adt/ri_triggers.c` at lines 743-848
- Returns a Datum value as required by PostgreSQL's function call interface
- Uses `SPI_OK_DELETE` as the expected result from the delete operation

## Simplified Source

```c
Datum
RI_FKey_cascade_del(PG_FUNCTION_ARGS)
{
    TriggerData *trigdata = (TriggerData *) fcinfo->context;
    const RI_ConstraintInfo *riinfo;
    Relation fk_rel;
    Relation pk_rel;
    TupleTableSlot *oldslot;
    RI_QueryKey qkey;
    SPIPlanPtr qplan;

    // Validate trigger call
    ri_CheckTrigger(fcinfo, "RI_FKey_cascade_del", RI_TRIGTYPE_DELETE);

    // Get constraint information and open relations
    riinfo = ri_FetchConstraintInfo(trigdata->tg_trigger, trigdata->tg_relation, true);
    fk_rel = table_open(riinfo->fk_relid, RowExclusiveLock);
    pk_rel = trigdata->tg_relation;
    oldslot = trigdata->tg_trigslot;

    SPI_connect();

    // Build or fetch cached query plan for cascaded delete
    ri_BuildQueryKey(&qkey, riinfo, RI_PLAN_CASCADE_ONDELETE);
    qplan = ri_FetchPreparedPlan(&qkey);

    if (qplan == NULL) {
        // Build DELETE query: "DELETE FROM [ONLY] <fktable> WHERE $1 = fkatt1 [AND ...]"
        StringInfoData querybuf;
        char fkrelname[MAX_QUOTED_REL_NAME_LEN];
        Oid queryoids[RI_MAX_NUMKEYS];

        initStringInfo(&querybuf);
        quoteRelationName(fkrelname, fk_rel);

        // Handle partitioned tables
        const char *fk_only = fk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ? "" : "ONLY ";
        appendStringInfo(&querybuf, "DELETE FROM %s%s", fk_only, fkrelname);

        // Build WHERE clause for each key column
        const char *querysep = "WHERE";
        for (int i = 0; i < riinfo->nkeys; i++) {
            char attname[MAX_QUOTED_NAME_LEN];
            char paramname[16];

            quoteOneName(attname, RIAttName(fk_rel, riinfo->fk_attnums[i]));
            sprintf(paramname, "$%d", i + 1);

            ri_GenerateQual(&querybuf, querysep, paramname,
                           RIAttType(pk_rel, riinfo->pk_attnums[i]),
                           riinfo->pf_eq_oprs[i], attname,
                           RIAttType(fk_rel, riinfo->fk_attnums[i]));

            querysep = "AND";
            queryoids[i] = RIAttType(pk_rel, riinfo->pk_attnums[i]);
        }

        // Prepare and cache the plan
        qplan = ri_PlanCheck(querybuf.data, riinfo->nkeys, queryoids, &qkey, fk_rel, pk_rel);
    }

    // Execute the cascaded delete using values from deleted PK tuple
    ri_PerformCheck(riinfo, &qkey, qplan, fk_rel, pk_rel, oldslot, NULL, true, SPI_OK_DELETE);

    SPI_finish();
    table_close(fk_rel, RowExclusiveLock);

    return PointerGetDatum(NULL);
}
```
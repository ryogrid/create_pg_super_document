# RI_FKey_cascade_upd

## Location
[src/backend/utils/adt/ri_triggers.c:849-969](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L849-L969)

## Overview
A trigger function that implements CASCADE behavior for UPDATE operations, automatically updating all foreign key references when a primary key value is updated in the referenced table.

## Definition
```c
Datum RI_FKey_cascade_upd(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL trigger function that enforces the CASCADE referential integrity constraint when primary key values are updated in a referenced (parent) table. When a CASCADE constraint is defined, updating a primary key should automatically update all corresponding foreign key values in referencing tables to maintain referential integrity.

The function builds and executes an UPDATE statement against the foreign key table to update all rows that reference the old primary key values with the new primary key values. The query constructed is of the form: `UPDATE [ONLY] <fktable> SET fkatt1 =  [, ...] WHERE  = fkatt1 [AND ...]`, where the first set of parameters contains the new primary key values and the WHERE clause parameters contain the old primary key values.

The function uses RowExclusiveLock on the foreign key relation since it will perform UPDATE operations, and requires both old and new tuple slots to extract the before and after primary key values.

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
  - [ri_PlanCheck](../r/ri_PlanCheck.md): Prepares and caches the UPDATE query plan
  - [ri_PerformCheck](../r/ri_PerformCheck.md): Executes the cascaded update operation
  - `[appendBinaryStringInfo](../a/appendBinaryStringInfo.md)`: Appends WHERE clause to UPDATE statement
  - SPI functions: `SPI_connect`, `SPI_finish`
  - Various utility functions for name quoting and type handling
  - `RI_PLAN_CASCADE_ONUPDATE`: Query plan type constant
  - `RI_TRIGTYPE_UPDATE`: UPDATE trigger type constant

- Called from (representative examples):
  - No direct callers found (invoked by PostgreSQL trigger system)

## Notes and Other Information
- This function is registered as a trigger function in the PostgreSQL system catalog
- Uses RowExclusiveLock mode on the foreign key relation since UPDATE operations will be performed
- Implements query plan caching for performance optimization using `RI_PLAN_CASCADE_ONUPDATE`
- Handles partitioned tables by omitting ONLY keyword when appropriate
- Requires twice as many parameters as other constraint functions (both old and new key values)
- The cascaded updates can trigger additional cascades if the foreign key tables have their own CASCADE constraints
- Assumes there is a valid assignment cast from the primary key type to the foreign key type
- Part of PostgreSQL's comprehensive referential integrity system
- Located in `src/backend/utils/adt/ri_triggers.c` at lines 849-969
- Returns a Datum value as required by PostgreSQL's function call interface
- Uses `SPI_OK_UPDATE` as the expected result from the update operation

## Simplified Source

```c
Datum
RI_FKey_cascade_upd(PG_FUNCTION_ARGS)
{
    TriggerData *trigdata = (TriggerData *) fcinfo->context;
    const RI_ConstraintInfo *riinfo;
    Relation fk_rel;
    Relation pk_rel;
    TupleTableSlot *newslot;
    TupleTableSlot *oldslot;
    RI_QueryKey qkey;
    SPIPlanPtr qplan;

    // Validate trigger call
    ri_CheckTrigger(fcinfo, "RI_FKey_cascade_upd", RI_TRIGTYPE_UPDATE);

    // Get constraint information and open relations
    riinfo = ri_FetchConstraintInfo(trigdata->tg_trigger, trigdata->tg_relation, true);
    fk_rel = table_open(riinfo->fk_relid, RowExclusiveLock);
    pk_rel = trigdata->tg_relation;
    newslot = trigdata->tg_newslot;
    oldslot = trigdata->tg_trigslot;

    SPI_connect();

    // Build or fetch cached query plan for cascaded update
    ri_BuildQueryKey(&qkey, riinfo, RI_PLAN_CASCADE_ONUPDATE);
    qplan = ri_FetchPreparedPlan(&qkey);

    if (qplan == NULL) {
        // Build UPDATE query: "UPDATE [ONLY] <fktable> SET fkatt1 = $1 [, ...] WHERE $n = fkatt1 [AND ...]"
        StringInfoData querybuf;
        StringInfoData qualbuf;
        char fkrelname[MAX_QUOTED_REL_NAME_LEN];
        Oid queryoids[RI_MAX_NUMKEYS * 2];

        initStringInfo(&querybuf);
        initStringInfo(&qualbuf);
        quoteRelationName(fkrelname, fk_rel);

        // Handle partitioned tables
        const char *fk_only = fk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ? "" : "ONLY ";
        appendStringInfo(&querybuf, "UPDATE %s%s SET", fk_only, fkrelname);

        // Build SET clause (new values) and WHERE clause (old values)
        const char *querysep = "";
        const char *qualsep = "WHERE";
        for (int i = 0, j = riinfo->nkeys; i < riinfo->nkeys; i++, j++) {
            char attname[MAX_QUOTED_NAME_LEN];
            char paramname[16];

            quoteOneName(attname, RIAttName(fk_rel, riinfo->fk_attnums[i]));

            // SET clause: fkatt1 = $1
            appendStringInfo(&querybuf, "%s %s = $%d", querysep, attname, i + 1);

            // WHERE clause: $n = fkatt1 (using old values)
            sprintf(paramname, "$%d", j + 1);
            ri_GenerateQual(&qualbuf, qualsep, paramname,
                           RIAttType(pk_rel, riinfo->pk_attnums[i]),
                           riinfo->pf_eq_oprs[i], attname,
                           RIAttType(fk_rel, riinfo->fk_attnums[i]));

            querysep = ",";
            qualsep = "AND";
            queryoids[i] = RIAttType(pk_rel, riinfo->pk_attnums[i]); // new values
            queryoids[j] = RIAttType(pk_rel, riinfo->pk_attnums[i]); // old values
        }

        // Combine SET and WHERE clauses
        appendBinaryStringInfo(&querybuf, qualbuf.data, qualbuf.len);

        // Prepare and cache the plan (needs 2x parameters: new + old values)
        qplan = ri_PlanCheck(querybuf.data, riinfo->nkeys * 2, queryoids, &qkey, fk_rel, pk_rel);
    }

    // Execute the cascaded update using old and new PK values
    ri_PerformCheck(riinfo, &qkey, qplan, fk_rel, pk_rel, oldslot, newslot, true, SPI_OK_UPDATE);

    SPI_finish();
    table_close(fk_rel, RowExclusiveLock);

    return PointerGetDatum(NULL);
}
```
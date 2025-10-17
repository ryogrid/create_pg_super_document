# ri_restrict

## Location
[src/backend/utils/adt/ri_triggers.c:624-742](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L624-L742)

## Overview
A core internal function that implements the common logic for both RESTRICT and NO ACTION referential integrity constraints for both DELETE and UPDATE operations on referenced tables.

## Definition
```c
static Datum ri_restrict(TriggerData *trigdata, bool is_no_action)
```

## Detailed Description
This function contains the shared implementation logic for four different foreign key constraint trigger functions: ON DELETE RESTRICT, ON DELETE NO ACTION, ON UPDATE RESTRICT, and ON UPDATE NO ACTION. It performs the actual constraint checking by querying the foreign key table to determine if any rows would become orphaned by the proposed operation.

The function builds and executes a SELECT query against the foreign key table to check for existing references to the key being modified. If references are found, it raises an error to prevent the constraint violation. The function handles the subtle difference between NO ACTION and RESTRICT constraints: in NO ACTION mode, it first checks if another primary key row with the same values already exists, which would make the constraint violation moot.

The query built is of the form: `SELECT 1 FROM [ONLY] <fktable> x WHERE  = fkatt1 [AND ...] FOR KEY SHARE OF x`, using the primary key values as parameters.

## Parameters / Member Variables
- `trigdata`: Trigger execution data containing context information including the trigger definition, target relation, and old tuple values
- `is_no_action`: Boolean flag distinguishing NO ACTION behavior from RESTRICT (true for NO ACTION, false for RESTRICT)

## Dependencies
- Functions called/Symbols referenced:
  - [ri_FetchConstraintInfo](ri_FetchConstraintInfo.md): Retrieves constraint metadata
  - `[table_open](../t/table_open.md)`: Opens the foreign key relation
  - [ri_Check_Pk_Match](ri_Check_Pk_Match.md): Checks for matching primary key (NO ACTION only)
  - [ri_BuildQueryKey](ri_BuildQueryKey.md): Builds query cache key
  - [ri_FetchPreparedPlan](ri_FetchPreparedPlan.md): Retrieves cached query plan
  - [ri_GenerateQual](ri_GenerateQual.md): Generates WHERE clause conditions
  - [ri_PlanCheck](ri_PlanCheck.md): Prepares and caches the query plan
  - [ri_PerformCheck](ri_PerformCheck.md): Executes the constraint check query
  - SPI functions: `SPI_connect`, `SPI_finish`
  - Various utility functions for name quoting and type handling

- Called from (representative examples):
  - [RI_FKey_noaction_del](../R/RI_FKey_noaction_del.md): NO ACTION DELETE constraints  
  - [RI_FKey_restrict_del](../R/RI_FKey_restrict_del.md): RESTRICT DELETE constraints
  - [RI_FKey_noaction_upd](../R/RI_FKey_noaction_upd.md): NO ACTION UPDATE constraints
  - [RI_FKey_restrict_upd](../R/RI_FKey_restrict_upd.md): RESTRICT UPDATE constraints
  - [ri_set](ri_set.md): SET NULL/DEFAULT constraint handling

## Notes and Other Information
- This is a static function, not directly accessible outside ri_triggers.c
- Uses SPI (Server Programming Interface) to execute SQL queries within trigger context
- Implements query plan caching for performance optimization
- Handles partitioned tables by omitting ONLY keyword when appropriate
- Takes RowShareLock on the foreign key relation for consistency
- Part of PostgreSQL's comprehensive referential integrity system
- Located in `src/backend/utils/adt/ri_triggers.c` at lines 624-742
- The function distinguishes between NO ACTION and RESTRICT primarily for the pk_match check optimization

## Simplified Source

```c
static Datum ri_restrict(TriggerData *trigdata, bool is_no_action) {
    const RI_ConstraintInfo *riinfo;
    Relation fk_rel, pk_rel;
    TupleTableSlot *oldslot;
    RI_QueryKey qkey;
    SPIPlanPtr qplan;

    // Get constraint info and open relations
    riinfo = ri_FetchConstraintInfo(trigdata->tg_trigger,
                                   trigdata->tg_relation, true);
    fk_rel = table_open(riinfo->fk_relid, RowShareLock);
    pk_rel = trigdata->tg_relation;
    oldslot = trigdata->tg_trigslot;

    // For NO ACTION: check if another PK row exists with same values
    if (is_no_action &&
        ri_Check_Pk_Match(pk_rel, fk_rel, oldslot, riinfo)) {
        table_close(fk_rel, RowShareLock);
        return PointerGetDatum(NULL);
    }

    // Connect to SPI for query operations
    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

    // Build query key for plan caching
    ri_BuildQueryKey(&qkey, riinfo, RI_PLAN_RESTRICT);

    // Get or create prepared plan
    if ((qplan = ri_FetchPreparedPlan(&qkey)) == NULL) {
        StringInfoData querybuf;
        char fkrelname[MAX_QUOTED_REL_NAME_LEN];
        Oid queryoids[RI_MAX_NUMKEYS];

        // Build SELECT query: SELECT 1 FROM <fktable> WHERE pk_vals = fk_cols FOR KEY SHARE
        initStringInfo(&querybuf);
        const char *fk_only = fk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ? "" : "ONLY ";

        quoteRelationName(fkrelname, fk_rel);
        appendStringInfo(&querybuf, "SELECT 1 FROM %s%s x", fk_only, fkrelname);

        // Add WHERE conditions for each key column
        const char *querysep = "WHERE";
        for (int i = 0; i < riinfo->nkeys; i++) {
            char attname[MAX_QUOTED_NAME_LEN];
            char paramname[16];
            Oid pk_type = RIAttType(pk_rel, riinfo->pk_attnums[i]);
            Oid fk_type = RIAttType(fk_rel, riinfo->fk_attnums[i]);

            quoteOneName(attname, RIAttName(fk_rel, riinfo->fk_attnums[i]));
            sprintf(paramname, "$%d", i + 1);
            ri_GenerateQual(&querybuf, querysep, paramname, pk_type,
                           riinfo->pf_eq_oprs[i], attname, fk_type);

            // Handle collation differences if needed
            Oid pk_coll = RIAttCollation(pk_rel, riinfo->pk_attnums[i]);
            Oid fk_coll = RIAttCollation(fk_rel, riinfo->fk_attnums[i]);
            if (pk_coll != fk_coll && !get_collation_isdeterministic(pk_coll))
                ri_GenerateQualCollation(&querybuf, pk_coll);

            querysep = "AND";
            queryoids[i] = pk_type;
        }
        appendStringInfoString(&querybuf, " FOR KEY SHARE OF x");

        // Prepare and cache the plan
        qplan = ri_PlanCheck(querybuf.data, riinfo->nkeys, queryoids,
                           &qkey, fk_rel, pk_rel);
    }

    // Execute query to check for existing foreign key references
    ri_PerformCheck(riinfo, &qkey, qplan, fk_rel, pk_rel,
                   oldslot, NULL, true, SPI_OK_SELECT);

    if (SPI_finish() != SPI_OK_FINISH)
        elog(ERROR, "SPI_finish failed");

    table_close(fk_rel, RowShareLock);
    return PointerGetDatum(NULL);
}
```
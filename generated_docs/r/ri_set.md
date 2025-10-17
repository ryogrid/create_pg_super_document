# ri_set

## Location
[src/backend/utils/adt/ri_triggers.c:1031-1225](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L1031-L1225)

## Overview
This is the core implementation function that handles SET NULL and SET DEFAULT actions for foreign key constraints on both DELETE and UPDATE operations.

## Definition

```c
static Datum
ri_set(TriggerData *trigdata, bool is_set_null, int tgkind)
```
## Detailed Description
ri_set is the central workhorse function that implements the actual logic for ON DELETE SET NULL, ON DELETE SET DEFAULT, ON UPDATE SET NULL, and ON UPDATE SET DEFAULT foreign key constraint actions. It dynamically builds and executes SQL UPDATE statements to modify foreign key values in the referencing table when the referenced primary key is deleted or updated. The function handles query plan caching, column-specific updates based on constraint configuration, and ensures referential integrity through validation checks.

## Parameters / Member Variables
- `*trigdata`: TriggerData structure containing trigger context information including relation references and tuple data
- `is_set_null`: Boolean flag indicating whether to set values to NULL (true) or DEFAULT (false)
- `tgkind`: Integer specifying the trigger type (RI_TRIGTYPE_DELETE or RI_TRIGTYPE_UPDATE)
## Dependencies
- Functions called/Symbols referenced:
  - [ri_FetchConstraintInfo](ri_FetchConstraintInfo.md) (retrieves constraint metadata)
  - [table_open](../t/table_open.md) (opens the foreign key relation with RowExclusiveLock)
  - [SPI_connect](../S/SPI_connect.md)/SPI_finish (SPI interface management)
  - [ri_BuildQueryKey](ri_BuildQueryKey.md)/ri_FetchPreparedPlan (query plan management)
  - [ri_PlanCheck](ri_PlanCheck.md) (prepares new query plans when needed)
  - [ri_PerformCheck](ri_PerformCheck.md) (executes the UPDATE statement)
  - [ri_restrict](ri_restrict.md) (performs additional validation for SET DEFAULT case)
  - Various utility functions: RIAttName, RIAttType, RIAttCollation, quoteRelationName, quoteOneName
- Called from (representative examples):
  - [RI_FKey_setnull_del](../R/RI_FKey_setnull_del.md)
  - [RI_FKey_setnull_upd](../R/RI_FKey_setnull_upd.md)
  - [RI_FKey_setdefault_del](../R/RI_FKey_setdefault_del.md)
  - [RI_FKey_setdefault_upd](../R/RI_FKey_setdefault_upd.md)

## Notes and Other Information
- The function supports both full and partial column updates based on confdelsetcols configuration
- [Query](../Q/Query.md) plans are cached for performance using the RI_QueryKey mechanism
- Handles partitioned tables by omitting the ONLY keyword when appropriate
- For SET DEFAULT operations, performs additional validation via ri_restrict to ensure no constraint violations
- Uses SPI (Server Programming Interface) to execute dynamically constructed UPDATE statements
- Located in src/backend/utils/adt/ri_triggers.c:1031-1225

## Simplified Source

```c
static Datum
ri_set(TriggerData *trigdata, bool is_set_null, int tgkind)
{
    const RI_ConstraintInfo *riinfo;
    Relation fk_rel;
    Relation pk_rel;
    TupleTableSlot *oldslot;
    RI_QueryKey qkey;
    SPIPlanPtr qplan;
    int32 queryno;

    // Get constraint information and open relations
    riinfo = ri_FetchConstraintInfo(trigdata->tg_trigger, trigdata->tg_relation, true);
    fk_rel = table_open(riinfo->fk_relid, RowExclusiveLock);
    pk_rel = trigdata->tg_relation;
    oldslot = trigdata->tg_trigslot;

    SPI_connect();

    // Determine query plan type based on trigger kind and set/null flag
    switch (tgkind) {
        case RI_TRIGTYPE_UPDATE:
            queryno = is_set_null ? RI_PLAN_SETNULL_ONUPDATE : RI_PLAN_SETDEFAULT_ONUPDATE;
            break;
        case RI_TRIGTYPE_DELETE:
            queryno = is_set_null ? RI_PLAN_SETNULL_ONDELETE : RI_PLAN_SETDEFAULT_ONDELETE;
            break;
        default:
            elog(ERROR, "invalid tgkind passed to ri_set");
    }

    // Build or fetch cached query plan
    ri_BuildQueryKey(&qkey, riinfo, queryno);
    qplan = ri_FetchPreparedPlan(&qkey);

    if (qplan == NULL) {
        // Build UPDATE query: "UPDATE [ONLY] <fktable> SET fkatt1 = {NULL|DEFAULT} [, ...] WHERE $1 = fkatt1 [AND ...]"
        StringInfoData querybuf;
        char fkrelname[MAX_QUOTED_REL_NAME_LEN];
        Oid queryoids[RI_MAX_NUMKEYS];

        // Determine which columns to update
        int num_cols_to_set;
        const int16 *set_cols;
        if (tgkind == RI_TRIGTYPE_DELETE && riinfo->ndelsetcols != 0) {
            // Use specific delete columns if configured
            num_cols_to_set = riinfo->ndelsetcols;
            set_cols = riinfo->confdelsetcols;
        } else {
            // Use all foreign key columns
            num_cols_to_set = riinfo->nkeys;
            set_cols = riinfo->fk_attnums;
        }

        initStringInfo(&querybuf);
        quoteRelationName(fkrelname, fk_rel);

        // Handle partitioned tables
        const char *fk_only = fk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ? "" : "ONLY ";
        appendStringInfo(&querybuf, "UPDATE %s%s SET", fk_only, fkrelname);

        // Build SET clause: fkatt1 = {NULL|DEFAULT} [, ...]
        const char *querysep = "";
        for (int i = 0; i < num_cols_to_set; i++) {
            char attname[MAX_QUOTED_NAME_LEN];
            quoteOneName(attname, RIAttName(fk_rel, set_cols[i]));
            appendStringInfo(&querybuf, "%s %s = %s", querysep, attname, is_set_null ? "NULL" : "DEFAULT");
            querysep = ",";
        }

        // Build WHERE clause: $1 = fkatt1 [AND ...]
        const char *qualsep = "WHERE";
        for (int i = 0; i < riinfo->nkeys; i++) {
            char attname[MAX_QUOTED_NAME_LEN];
            char paramname[16];

            quoteOneName(attname, RIAttName(fk_rel, riinfo->fk_attnums[i]));
            sprintf(paramname, "$%d", i + 1);

            ri_GenerateQual(&querybuf, qualsep, paramname,
                           RIAttType(pk_rel, riinfo->pk_attnums[i]),
                           riinfo->pf_eq_oprs[i], attname,
                           RIAttType(fk_rel, riinfo->fk_attnums[i]));

            qualsep = "AND";
            queryoids[i] = RIAttType(pk_rel, riinfo->pk_attnums[i]);
        }

        // Prepare and cache the plan
        qplan = ri_PlanCheck(querybuf.data, riinfo->nkeys, queryoids, &qkey, fk_rel, pk_rel);
    }

    // Execute the SET NULL/DEFAULT operation
    ri_PerformCheck(riinfo, &qkey, qplan, fk_rel, pk_rel, oldslot, NULL, true, SPI_OK_UPDATE);

    SPI_finish();
    table_close(fk_rel, RowExclusiveLock);

    // For SET DEFAULT, perform additional validation check
    if (is_set_null) {
        return PointerGetDatum(NULL);
    } else {
        // Additional check needed for SET DEFAULT to ensure no constraint violations
        return ri_restrict(trigdata, true);
    }
}
```
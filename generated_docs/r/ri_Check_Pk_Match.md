# ri_Check_Pk_Match

## Location
[src/backend/utils/adt/ri_triggers.c:461-550](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L461-L550)

## Overview
Internal static function that checks if another primary key row exists with the same key values as a modified or deleted tuple, used to determine if foreign key constraint violations would occur.

## Definition

```c
static bool
ri_Check_Pk_Match(Relation pk_rel, Relation fk_rel,
				  TupleTableSlot *oldslot,
				  const RI_ConstraintInfo *riinfo)
```
## Detailed Description
This function performs a critical check in PostgreSQL's referential integrity system by searching the primary key table to determine if there's another row that matches the key values from a tuple that's being modified or deleted. This check is essential for NO ACTION and RESTRICT foreign key constraints to determine whether the operation should be allowed or blocked.

The function dynamically builds and executes a SELECT query against the primary key table using the values from the old tuple. It uses prepared statements for performance optimization and employs proper locking (FOR KEY SHARE) to ensure consistency. The function assumes the caller has already verified that the old tuple contains no NULL key values, as a match would be impossible with NULLs.

Key aspects of the implementation:
- Builds SQL query: "SELECT 1 FROM [ONLY] <pktable> x WHERE pkatt1 =  [AND ...] FOR KEY SHARE OF x"
- Uses SPI (Server Programming Interface) for query execution
- Supports both regular tables and partitioned tables
- Uses prepared statement caching for performance

## Parameters / Member Variables
- `pk_rel`: Relation pointer to the primary key table being checked
- `fk_rel`: Relation pointer to the foreign key table (used for plan caching)
- `*oldslot`: TupleTableSlot containing the tuple values to match against
- `*riinfo`: RI_ConstraintInfo structure containing constraint metadata including key column mappings
## Dependencies
- Functions called/Symbols referenced:
  - [ri_NullCheck](ri_NullCheck.md)
  - [SPI_connect](../S/SPI_connect.md)/SPI_finish
  - [ri_BuildQueryKey](ri_BuildQueryKey.md)
  - [ri_FetchPreparedPlan](ri_FetchPreparedPlan.md)
  - [ri_PlanCheck](ri_PlanCheck.md)
  - [ri_PerformCheck](ri_PerformCheck.md)
  - [quoteRelationName](../q/quoteRelationName.md)
  - [quoteOneName](../q/quoteOneName.md)
  - RIAttType
  - RIAttName
  - [ri_GenerateQual](ri_GenerateQual.md)
  - RI_PLAN_CHECK_LOOKUPPK_FROM_PK (constant)
  - Various SPI constants and types
- Called from (representative examples):
  - [ri_restrict](ri_restrict.md) (src/backend/utils/adt/ri_triggers.c:653)

## Notes and Other Information
- This is a static (internal) function, not exposed outside ri_triggers.c
- Function assumes input tuple has no NULL values in key columns (verified by Assert)
- Uses FOR KEY SHARE locking to prevent concurrent modifications during the check
- Returns boolean: true if a matching primary key row is found, false otherwise
- Critical component in implementing NO ACTION and RESTRICT foreign key constraints
- Located in src/backend/utils/adt/ri_triggers.c:461-550
- Handles both regular and partitioned primary key tables appropriately
- Part of PostgreSQL's comprehensive referential integrity enforcement system

## Simplified Source

```c
static bool ri_Check_Pk_Match(Relation pk_rel, Relation fk_rel,
                              TupleTableSlot *oldslot,
                              const RI_ConstraintInfo *riinfo) {
    SPIPlanPtr qplan;
    RI_QueryKey qkey;
    bool result;

    // Verify old tuple has no NULL key values
    Assert(ri_NullCheck(RelationGetDescr(pk_rel), oldslot, riinfo, true) == RI_KEYS_NONE_NULL);

    // Connect to SPI for query execution
    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

    // Build query key for plan caching
    ri_BuildQueryKey(&qkey, riinfo, RI_PLAN_CHECK_LOOKUPPK_FROM_PK);

    // Get or create prepared plan
    if ((qplan = ri_FetchPreparedPlan(&qkey)) == NULL) {
        StringInfoData querybuf;
        char pkrelname[MAX_QUOTED_REL_NAME_LEN];
        Oid queryoids[RI_MAX_NUMKEYS];

        // Build SELECT query: SELECT 1 FROM <pktable> WHERE key_cols = values FOR KEY SHARE
        initStringInfo(&querybuf);
        const char *pk_only = pk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ? "" : "ONLY ";

        quoteRelationName(pkrelname, pk_rel);
        appendStringInfo(&querybuf, "SELECT 1 FROM %s%s x", pk_only, pkrelname);

        // Add WHERE conditions for each key column
        const char *querysep = "WHERE";
        for (int i = 0; i < riinfo->nkeys; i++) {
            char attname[MAX_QUOTED_NAME_LEN];
            char paramname[16];
            Oid pk_type = RIAttType(pk_rel, riinfo->pk_attnums[i]);

            quoteOneName(attname, RIAttName(pk_rel, riinfo->pk_attnums[i]));
            sprintf(paramname, "$%d", i + 1);
            ri_GenerateQual(&querybuf, querysep, attname, pk_type,
                           riinfo->pp_eq_oprs[i], paramname, pk_type);
            querysep = "AND";
            queryoids[i] = pk_type;
        }
        appendStringInfoString(&querybuf, " FOR KEY SHARE OF x");

        // Prepare and cache the plan
        qplan = ri_PlanCheck(querybuf.data, riinfo->nkeys, queryoids,
                           &qkey, fk_rel, pk_rel);
    }

    // Execute the query to check for matching primary key
    result = ri_PerformCheck(riinfo, &qkey, qplan, fk_rel, pk_rel,
                           oldslot, NULL, true, SPI_OK_SELECT);

    if (SPI_finish() != SPI_OK_FINISH)
        elog(ERROR, "SPI_finish failed");

    return result;
}
```
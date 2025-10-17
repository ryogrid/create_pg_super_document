# ri_PerformCheck

## Location
[src/backend/utils/adt/ri_triggers.c:2312-2448](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L2312-L2448)

## Overview
Performs a query to enforce a referential integrity restriction by executing a pre-planned SPI query with appropriate snapshots and security contexts.

## Definition

```c
static bool
ri_PerformCheck(const RI_ConstraintInfo *riinfo,
				RI_QueryKey *qkey, SPIPlanPtr qplan,
				Relation fk_rel, Relation pk_rel,
				TupleTableSlot *oldslot, TupleTableSlot *newslot,
				bool detectNewRows, int expect_OK)
```
## Detailed Description
This is a core function in PostgreSQL's referential integrity enforcement system. It executes pre-compiled SPI queries to check foreign key constraints, handling various constraint actions like RESTRICT, CASCADE, SET NULL, etc. The function manages transaction snapshots appropriately for different isolation levels, switches to the table owner's security context for permission checks, and extracts key values from tuple slots to use as query parameters.

The function determines whether to query the primary key or foreign key table based on the query type, extracts the appropriate values from the source tuples, manages snapshots for consistency in different isolation levels, and executes the query with proper security context switching.

## Parameters / Member Variables
- `*riinfo`: Constraint information structure containing details about the foreign key relationship
- `*qkey`: Query key identifying the specific type of RI query to execute
- `qplan`: Pre-compiled SPI plan for the query to be executed
- `fk_rel`: Foreign key table relation
- `pk_rel`: Primary key table relation
- `*oldslot`: Tuple slot containing the old tuple values (for updates/deletes)
- `*newslot`: Tuple slot containing the new tuple values (for inserts/updates)
- `detectNewRows`: Whether to detect rows that became visible after transaction start
- `expect_OK`: Expected SPI result code for validation
## Dependencies
- Functions called/Symbols referenced:
  - [ri_ExtractValues](ri_ExtractValues.md)
  - [ri_ReportViolation](ri_ReportViolation.md)
  - IsolationUsesXactSnapshot
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - [GetLatestSnapshot](../G/GetLatestSnapshot.md)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)
  - [GetUserIdAndSecContext](../G/GetUserIdAndSecContext.md)
  - [SetUserIdAndSecContext](../S/SetUserIdAndSecContext.md)
  - RelationGetForm
  - [SPI_execute_snapshot](../S/SPI_execute_snapshot.md)
  - [SPI_result_code_string](../S/SPI_result_code_string.md)
- Called from (representative examples):
  - [ri_Check_Pk_Match](ri_Check_Pk_Match.md)
  - [ri_restrict](ri_restrict.md)
  - [RI_FKey_cascade_del](../R/RI_FKey_cascade_del.md)
  - [RI_FKey_cascade_upd](../R/RI_FKey_cascade_upd.md)
  - [ri_set](ri_set.md)

## Notes and Other Information
- Handles snapshot management differently based on isolation level to ensure consistency
- Switches to table owner's security context to perform permission checks as the appropriate user
- Returns true if the query found matching rows, false otherwise
- May report constraint violations through ri_ReportViolation when appropriate
- Uses pre-compiled SPI plans for performance optimization
- Supports various referential integrity actions through different query types

## Simplified Source

```c
static bool
ri_PerformCheck(const RI_ConstraintInfo *riinfo, RI_QueryKey *qkey, SPIPlanPtr qplan,
                Relation fk_rel, Relation pk_rel, TupleTableSlot *oldslot,
                TupleTableSlot *newslot, bool detectNewRows, int expect_OK)
{
    Relation query_rel, source_rel;
    bool source_is_pk;
    Snapshot test_snapshot, crosscheck_snapshot;
    int limit, spi_result;
    Oid save_userid;
    int save_sec_context;
    Datum vals[RI_MAX_NUMKEYS * 2];
    char nulls[RI_MAX_NUMKEYS * 2];

    // Determine which table to query based on query type
    if (qkey->constr_queryno <= RI_PLAN_LAST_ON_PK)
        query_rel = pk_rel;
    else
        query_rel = fk_rel;

    // Determine source table for extracting values
    if (qkey->constr_queryno == RI_PLAN_CHECK_LOOKUPPK) {
        source_rel = fk_rel;
        source_is_pk = false;
    } else {
        source_rel = pk_rel;
        source_is_pk = true;
    }

    // Extract parameter values from tuple slots
    if (newslot) {
        ri_ExtractValues(source_rel, newslot, riinfo, source_is_pk, vals, nulls);
        if (oldslot)
            ri_ExtractValues(source_rel, oldslot, riinfo, source_is_pk,
                           vals + riinfo->nkeys, nulls + riinfo->nkeys);
    } else {
        ri_ExtractValues(source_rel, oldslot, riinfo, source_is_pk, vals, nulls);
    }

    // Set up snapshots for transaction isolation
    if (IsolationUsesXactSnapshot() && detectNewRows) {
        CommandCounterIncrement();
        test_snapshot = GetLatestSnapshot();
        crosscheck_snapshot = GetTransactionSnapshot();
    } else {
        test_snapshot = InvalidSnapshot;
        crosscheck_snapshot = InvalidSnapshot;
    }

    // Set query limit (1 for SELECT queries, 0 for modification queries)
    limit = (expect_OK == SPI_OK_SELECT) ? 1 : 0;

    // Switch to table owner's security context
    GetUserIdAndSecContext(&save_userid, &save_sec_context);
    SetUserIdAndSecContext(RelationGetForm(query_rel)->relowner,
                          save_sec_context | SECURITY_LOCAL_USERID_CHANGE |
                          SECURITY_NOFORCE_RLS);

    // Execute the query
    spi_result = SPI_execute_snapshot(qplan, vals, nulls, test_snapshot,
                                     crosscheck_snapshot, false, false, limit);

    // Restore original security context
    SetUserIdAndSecContext(save_userid, save_sec_context);

    // Validate result
    if (spi_result < 0)
        elog(ERROR, "SPI_execute_snapshot returned %s", SPI_result_code_string(spi_result));

    if (expect_OK >= 0 && spi_result != expect_OK)
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                       errmsg("referential integrity query gave unexpected result")));

    // Report violations if needed
    if (qkey->constr_queryno != RI_PLAN_CHECK_LOOKUPPK_FROM_PK &&
        expect_OK == SPI_OK_SELECT &&
        (SPI_processed == 0) == (qkey->constr_queryno == RI_PLAN_CHECK_LOOKUPPK))
        ri_ReportViolation(riinfo, pk_rel, fk_rel, newslot ? newslot : oldslot,
                          NULL, qkey->constr_queryno, false);

    return SPI_processed != 0;
}
```
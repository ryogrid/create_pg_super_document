# vacuum

## Location
[src/backend/commands/vacuum.c:479-716](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L479-L716)

## Overview
Internal entry point for autovacuum and the VACUUM/ANALYZE commands that orchestrates the processing of specified relations or all relevant tables in the database.

## Definition

```c
void
vacuum(List *relations, VacuumParams *params, BufferAccessStrategy bstrategy,
	   MemoryContext vac_context, bool isTopLevel)
```
## Detailed Description
The vacuum function serves as the core orchestration layer for both user-initiated and automatic vacuum/analyze operations. It handles transaction management, relation list processing, and coordinates the execution of vacuum and analyze operations across multiple relations.

Key responsibilities include:
- Transaction boundary management (determining when to use separate transactions vs. the outer transaction)
- Preventing recursive vacuum calls that could occur through hostile index expressions
- Building and expanding the list of relations to process based on input parameters
- Managing vacuum cost accounting and failsafe mechanisms
- Coordinating vacuum_rel() and analyze_rel() calls for each target relation
- Updating database-wide statistics (pg_database.datfrozenxid) after vacuum operations

The function implements sophisticated transaction handling logic: VACUUM operations always use separate transactions to release locks quickly, while ANALYZE operations may reuse the outer transaction depending on context. For autovacuum workers and multi-relation operations, separate transactions are preferred for better concurrency.

## Parameters / Member Variables
- : List of VacuumRelation structures to process, or NIL to process all relevant tables in the database
- : VacuumParams structure containing options and configuration parameters for the operation
- : BufferAccessStrategy for controlling shared buffer usage, or NULL for unrestricted access
- : MemoryContext for allocating vacuum-related data that persists across transactions
- : Boolean indicating if this is a top-level command (affects transaction block validation)

## Dependencies
- Functions called/Symbols referenced:
  - [vacuum_rel](vacuum_rel.md) (per-relation vacuum processing)
  - [analyze_rel](../a/analyze_rel.md) (per-relation analysis processing)
  - [expand_vacuum_rel](../e/expand_vacuum_rel.md) (relation expansion utility)
  - [get_all_vacuum_rels](../g/get_all_vacuum_rels.md) (database-wide relation discovery)
  - [PreventInTransactionBlock](../P/PreventInTransactionBlock.md) (transaction validation)
  - [VacuumUpdateCosts](../V/VacuumUpdateCosts.md) (cost accounting initialization)
  - [vac_update_datfrozenxid](vac_update_datfrozenxid.md) (database statistics updates)
- Called from (representative examples):
  - [ExecVacuum](../E/ExecVacuum.md) (user command entry point)
  - [autovacuum_do_vac_analyze](../a/autovacuum_do_vac_analyze.md) (autovacuum worker)

## Notes and Other Information
- Uses static variable in_vacuum to prevent recursive calls that could occur through index expressions
- Implements comprehensive transaction management: VACUUM always uses separate transactions, ANALYZE behavior depends on context
- Maintains vacuum cost accounting variables (VacuumPageHit, VacuumPageMiss, VacuumPageDirty) for throttling
- Supports database-only statistics mode (VACOPT_ONLY_DATABASE_STATS) that skips table processing
- Uses PG_TRY/PG_FINALLY blocks to ensure proper cleanup of vacuum state variables
- Handles both single-relation and multi-relation operations with appropriate transaction boundaries
- Updates system-wide frozen transaction ID tracking after vacuum operations complete

## Simplified Source

```c
void
vacuum(List *relations, VacuumParams *params, BufferAccessStrategy bstrategy,
       MemoryContext vac_context, bool isTopLevel)
{
    static bool in_vacuum = false;
    const char *stmttype = (params->options & VACOPT_VACUUM) ? "VACUUM" : "ANALYZE";
    bool in_outer_xact, use_own_xacts;

    // Transaction validation for VACUUM operations
    if (params->options & VACOPT_VACUUM) {
        PreventInTransactionBlock(isTopLevel, stmttype);
        in_outer_xact = false;
    } else {
        in_outer_xact = IsInTransactionBlock(isTopLevel);
    }

    // Prevent recursive vacuum calls
    if (in_vacuum)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("%s cannot be executed from VACUUM or ANALYZE", stmttype)));

    // Build relation list
    if (params->options & VACOPT_ONLY_DATABASE_STATS) {
        // Database stats only, no table processing
        Assert(relations == NIL);
    } else if (relations != NIL) {
        // Expand provided relation list
        List *newrels = NIL;
        ListCell *lc;
        foreach(lc, relations) {
            VacuumRelation *vrel = lfirst_node(VacuumRelation, lc);
            List *sublist = expand_vacuum_rel(vrel, vac_context, params->options);
            MemoryContext old_context = MemoryContextSwitchTo(vac_context);
            newrels = list_concat(newrels, sublist);
            MemoryContextSwitchTo(old_context);
        }
        relations = newrels;
    } else {
        // Get all vacuum-eligible relations
        relations = get_all_vacuum_rels(vac_context, params->options);
    }

    // Decide on transaction strategy
    if (params->options & VACOPT_VACUUM) {
        use_own_xacts = true;  // VACUUM always uses separate transactions
    } else {
        // ANALYZE transaction strategy depends on context
        if (AmAutoVacuumWorkerProcess())
            use_own_xacts = true;
        else if (in_outer_xact)
            use_own_xacts = false;
        else if (list_length(relations) > 1)
            use_own_xacts = true;
        else
            use_own_xacts = false;
    }

    // Start transaction management
    if (use_own_xacts) {
        Assert(!in_outer_xact);
        if (ActiveSnapshotSet())
            PopActiveSnapshot();
        CommitTransactionCommand();
    }

    // Main vacuum/analyze processing
    PG_TRY();
    {
        ListCell *cur;

        in_vacuum = true;
        VacuumFailsafeActive = false;
        VacuumUpdateCosts();
        VacuumCostBalance = 0;

        // Process each relation
        foreach(cur, relations) {
            VacuumRelation *vrel = lfirst_node(VacuumRelation, cur);

            if (params->options & VACOPT_VACUUM) {
                VacuumParams params_copy;
                memcpy(&params_copy, params, sizeof(VacuumParams));
                if (!vacuum_rel(vrel->oid, vrel->relation, &params_copy, bstrategy))
                    continue;
            }

            if (params->options & VACOPT_ANALYZE) {
                if (use_own_xacts) {
                    StartTransactionCommand();
                    PushActiveSnapshot(GetTransactionSnapshot());
                }

                analyze_rel(vrel->oid, vrel->relation, params,
                           vrel->va_cols, in_outer_xact, bstrategy);

                if (use_own_xacts) {
                    PopActiveSnapshot();
                    CommandCounterIncrement();
                    CommitTransactionCommand();
                } else {
                    CommandCounterIncrement();
                }
            }

            VacuumFailsafeActive = false;
        }
    }
    PG_FINALLY();
    {
        in_vacuum = false;
        VacuumCostActive = false;
        VacuumFailsafeActive = false;
        VacuumCostBalance = 0;
    }
    PG_END_TRY();

    // Finish transaction management
    if (use_own_xacts) {
        StartTransactionCommand();
    }

    // Update database-wide statistics if needed
    if ((params->options & VACOPT_VACUUM) &&
        !(params->options & VACOPT_SKIP_DATABASE_STATS)) {
        vac_update_datfrozenxid();
    }
}
```
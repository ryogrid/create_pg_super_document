# _SPI_execute_plan

## Location
[src/backend/executor/spi.c:2399-2848](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L2399-L2848)

## Overview
_SPI_execute_plan is the core internal function that executes prepared SQL plans with comprehensive options for controlling execution behavior, snapshot management, and result handling.

## Definition
```c
static int _SPI_execute_plan(SPIPlanPtr plan, const SPIExecuteOptions *options,
                            Snapshot snapshot, Snapshot crosscheck_snapshot,
                            bool fire_triggers)
```

## Detailed Description
The _SPI_execute_plan function is the central execution engine for the Server Programming Interface (SPI). It handles the complete execution lifecycle of prepared SQL plans, including snapshot management, parameter binding, command execution, and result collection.

The function supports four distinct snapshot management behaviors based on the provided snapshot parameter and read-only mode. It handles both regular prepared plans and one-shot plans, performing deferred parse analysis for the latter. The function manages atomic and non-atomic execution contexts, enforces read-only restrictions, and processes both utility statements and planned queries.

For each statement in the plan, it sets up appropriate destination receivers, manages command counter increments, handles transaction semantics, and collects execution results. The function provides comprehensive error handling and resource cleanup.

## Parameters / Member Variables
- `plan`: SPIPlanPtr containing the prepared plan to execute
- `options`: SPIExecuteOptions structure containing execution parameters including params, read_only flag, tuple count limit, and destination receiver
- `snapshot`: Query snapshot to use, or InvalidSnapshot for normal snapshot behavior  
- `crosscheck_snapshot`: Snapshot for referential integrity checks, typically InvalidSnapshot
- `fire_triggers`: Whether to fire AFTER triggers at query end (true) or postpone to outer query (false)

## Dependencies
- Functions called/Symbols referenced:
  - [IsSubTransaction](../I/IsSubTransaction.md)
  - [PushActiveSnapshot](../P/PushActiveSnapshot.md)/PopActiveSnapshot
  - [GetCachedPlan](../G/GetCachedPlan.md)/ReleaseCachedPlan
  - [CreateQueryDesc](../C/CreateQueryDesc.md)/FreeQueryDesc
  - [ProcessUtility](../P/ProcessUtility.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - [_SPI_pquery](_SPI_pquery.md)
  - [pg_analyze_and_rewrite_withcb](../p/pg_analyze_and_rewrite_withcb.md)
  - [pg_analyze_and_rewrite_fixedparams](../p/pg_analyze_and_rewrite_fixedparams.md)
  - [CompleteCachedPlan](../C/CompleteCachedPlan.md)
- Called from (representative examples):
  - [SPI_execute](SPI_execute.md)
  - [SPI_execute_extended](SPI_execute_extended.md)
  - [SPI_execute_plan](SPI_execute_plan.md)
  - [SPI_execute_plan_extended](SPI_execute_plan_extended.md)
  - [SPI_execute_with_args](SPI_execute_with_args.md)

## Notes and Other Information
- Returns SPI result codes (SPI_OK_*, SPI_ERROR_*)
- Sets global SPI_processed and SPI_tuptable variables for caller access
- Supports both atomic and non-atomic execution contexts based on connection options
- Handles one-shot plans by performing deferred parse analysis during execution
- Manages complex snapshot semantics for different read/write and atomic/non-atomic combinations
- Validates that must_return_tuples queries actually return tuples
- Prevents execution of unsupported statements like COPY without filename or transaction statements
- Updates command counter between statements in write mode for visibility
- Transfers tuple table ownership from SPI context to caller

## Simplified Source

```c
static int _SPI_execute_plan(SPIPlanPtr plan, const SPIExecuteOptions *options,
                           Snapshot snapshot, Snapshot crosscheck_snapshot,
                           bool fire_triggers) {
    int my_res = 0;
    uint64 my_processed = 0;
    SPITupleTable *my_tuptable = NULL;
    bool allow_nonatomic = options->allow_nonatomic &&
                          !_SPI_current->atomic && !IsSubTransaction();
    bool pushed_active_snap = false;
    ResourceOwner plan_owner = options->owner;
    CachedPlan *cplan = NULL;

    // Setup error tracking
    // ... error context setup ...

    // Handle snapshot management based on mode
    if (snapshot != InvalidSnapshot) {
        if (options->read_only) {
            PushActiveSnapshot(snapshot);
        } else {
            PushCopiedSnapshot(snapshot);
        }
        pushed_active_snap = true;
    }

    // Ensure proper resource owner for saved plans
    if (!plan->saved) {
        plan_owner = NULL;
    } else if (plan_owner == NULL) {
        plan_owner = CurrentResourceOwner;
    }

    // Validate must_return_tuples requirement
    if (options->must_return_tuples && plan->plancache_list == NIL) {
        ereport(ERROR, "empty query does not return tuples");
    }

    // Execute each cached plan source
    foreach(lc1, plan->plancache_list) {
        CachedPlanSource *plansource = lfirst(lc1);

        // Handle one-shot plans with deferred parsing
        if (plan->oneshot) {
            // Parse and analyze the query
            querytree_list = pg_analyze_and_rewrite_*(...);
            CompleteCachedPlan(plansource, querytree_list, ...);
        }

        // Validate tuple return requirement for this plan
        if (options->must_return_tuples && !plansource->resultDesc) {
            ereport(ERROR, "%s query does not return tuples", cmdtag);
        }

        // Get cached plan and statement list
        cplan = GetCachedPlan(plansource, options->params,
                             plan_owner, _SPI_current->queryEnv);
        stmt_list = cplan->stmt_list;

        // Setup snapshot for statement list if needed
        if (snapshot == InvalidSnapshot && statements_need_snapshot) {
            EnsurePortalSnapshotExists();
            if (!options->read_only && !allow_nonatomic) {
                PushActiveSnapshot(GetTransactionSnapshot());
                pushed_active_snap = true;
            }
        }

        // Execute each statement in the list
        foreach(lc2, stmt_list) {
            PlannedStmt *stmt = lfirst_node(PlannedStmt, lc2);
            bool canSetTag = stmt->canSetTag;

            // Reset per-statement state
            _SPI_current->processed = 0;
            _SPI_current->tuptable = NULL;

            // Check for unsupported statements
            if (stmt->utilityStmt) {
                if (IsA(stmt->utilityStmt, CopyStmt) ||
                    IsA(stmt->utilityStmt, TransactionStmt)) {
                    my_res = SPI_ERROR_*;
                    goto fail;
                }
            }

            // Enforce read-only restrictions
            if (options->read_only && !CommandIsReadOnly(stmt)) {
                ereport(ERROR, "%s is not allowed in non-volatile function");
            }

            // Update command counter and snapshot for write operations
            if (!options->read_only && pushed_active_snap) {
                CommandCounterIncrement();
                UpdateActiveSnapshotCommandId();
            }

            // Setup destination receiver
            if (!canSetTag) {
                dest = CreateDestReceiver(DestNone);
            } else if (options->dest) {
                dest = options->dest;
            } else {
                dest = CreateDestReceiver(DestSPI);
            }

            // Execute statement
            if (stmt->utilityStmt == NULL) {
                // Regular planned statement
                qdesc = CreateQueryDesc(stmt, plansource->query_string,
                                       snap, crosscheck_snapshot, dest, ...);
                res = _SPI_pquery(qdesc, fire_triggers,
                                 canSetTag ? options->tcount : 0);
                FreeQueryDesc(qdesc);
            } else {
                // Utility statement
                ProcessUtility(stmt, plansource->query_string, true,
                              context, options->params, ..., dest, &qc);

                // Handle special utility statement results
                if (IsA(stmt->utilityStmt, CreateTableAsStmt)) {
                    // Handle CREATE TABLE AS result counting
                } else if (IsA(stmt->utilityStmt, CopyStmt)) {
                    _SPI_current->processed = qc.nprocessed;
                }
                res = SPI_OK_UTILITY;
            }

            // Collect results from canSetTag statements
            if (canSetTag) {
                my_processed = _SPI_current->processed;
                SPI_freetuptable(my_tuptable);
                my_tuptable = _SPI_current->tuptable;
                my_res = res;
            } else {
                SPI_freetuptable(_SPI_current->tuptable);
                _SPI_current->tuptable = NULL;
            }

            if (res < 0) {
                my_res = res;
                goto fail;
            }
        }

        // Release plan and advance command counter
        ReleaseCachedPlan(cplan, plan_owner);
        cplan = NULL;

        if (!options->read_only) {
            CommandCounterIncrement();
        }
    }

fail:
    // Cleanup resources
    if (pushed_active_snap) {
        PopActiveSnapshot();
    }
    if (cplan) {
        ReleaseCachedPlan(cplan, plan_owner);
    }

    // Set results for caller
    SPI_processed = my_processed;
    SPI_tuptable = my_tuptable;
    _SPI_current->tuptable = NULL;

    if (my_res == 0) {
        my_res = SPI_OK_REWRITTEN;
    }

    return my_res;
}
```
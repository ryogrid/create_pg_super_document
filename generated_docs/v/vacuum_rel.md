# vacuum_rel

## Location
[src/backend/commands/vacuum.c:1973-2318](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L1973-L2318)

## Overview
Vacuums a single heap relation by handling transaction management, locking, privilege checking, and dispatching to appropriate vacuum implementations (full or lazy), with support for TOAST table processing.

## Definition

```c
static bool
vacuum_rel(Oid relid, RangeVar *relation, VacuumParams *params,
		   BufferAccessStrategy bstrategy)
```
## Detailed Description
This function orchestrates the vacuuming of a single relation through a complex process that ensures safety, proper locking, and comprehensive cleanup. The key aspects include:

1. **Transaction Management**: Starts a new transaction for the vacuum operation and manages transaction state throughout
2. **Process State Management**: Sets PROC_IN_VACUUM and PROC_VACUUM_FOR_WRAPAROUND flags for lazy vacuum to coordinate with other operations
3. **Snapshot Management**: Acquires snapshots to prevent pg_subtrans truncation and maintain proper visibility horizons
4. **Lock Management**: Acquires appropriate locks (ShareUpdateExclusiveLock for lazy vacuum, AccessExclusiveLock for full vacuum) and session-level locks
5. **Privilege and Validity Checking**: Validates permissions, relation types, and handles special cases like temp tables and partitioned tables
6. **Parameter Resolution**: Resolves vacuum options (index_cleanup, truncate) based on relation-specific settings when not explicitly specified
7. **Security Context Management**: Switches to table owner's userid for index function execution with restricted operations
8. **Vacuum Dispatch**: Calls either cluster_rel() for VACUUM FULL or table_relation_vacuum() for lazy vacuum
9. **TOAST Processing**: Recursively processes TOAST tables when requested while maintaining proper locking and privilege context

The function implements various safety checks and early exit conditions for unsupported relation types, privilege violations, or special cases like partitioned tables.

## Parameters / Member Variables
- `relid`: Object identifier of the relation to vacuum
- `*relation`: RangeVar containing the relation name (used only for error reporting; may be stale)
- `*params`: VacuumParams structure containing vacuum options and configuration
- `bstrategy`: Buffer access strategy to use during vacuum operations
## Dependencies
- Functions called/Symbols referenced:
  - [StartTransactionCommand](../S/StartTransactionCommand.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
  - [PushActiveSnapshot](../P/PushActiveSnapshot.md)
  - [PopActiveSnapshot](../P/PopActiveSnapshot.md)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)
  - [vacuum_open_relation](vacuum_open_relation.md)
  - [vacuum_is_permitted_for_relation](vacuum_is_permitted_for_relation.md)
  - [relation_close](../r/relation_close.md)
  - [LockRelationIdForSession](../L/LockRelationIdForSession.md)
  - [UnlockRelationIdForSession](../U/UnlockRelationIdForSession.md)
  - [GetUserIdAndSecContext](../G/GetUserIdAndSecContext.md)
  - [SetUserIdAndSecContext](../S/SetUserIdAndSecContext.md)
  - [NewGUCNestLevel](../N/NewGUCNestLevel.md)
  - [RestrictSearchPath](../R/RestrictSearchPath.md)
  - [AtEOXact_GUC](../A/AtEOXact_GUC.md)
  - [cluster_rel](../c/cluster_rel.md)
  - [table_relation_vacuum](../t/table_relation_vacuum.md)
- Called from (representative examples):
  - [vacuum](vacuum.md) (main vacuum entry point)
  - [vacuum_rel](vacuum_rel.md) (recursive call for TOAST tables)

## Notes and Other Information
- This is a static function, only accessible within the vacuum.c source file
- Returns true if it's safe to proceed with ANALYZE on the table, false otherwise
- The function is designed to work across multiple small transactions to avoid locking the entire database
- Implements a "one heap at a time" approach with overhead trade-offs for better concurrency
- Special handling for partitioned tables (skips actual vacuum work but allows ANALYZE)
- Handles both VACUUM FULL (via cluster_rel) and lazy vacuum (via table_relation_vacuum)
- TOAST table vacuum skips ANALYZE since statistics are not important for TOAST relations
- Uses injection points for testing when compiled with USE_INJECTION_POINTS
- Maintains session-level locks across the entire operation including TOAST processing
- The function must be called outside of any existing transaction context
- Parameter modification is done on a copy to avoid affecting TOAST table processing
- Security context is properly restored even if operations fail or are interrupted

## Simplified Source

```c
static bool vacuum_rel(Oid relid, RangeVar *relation, VacuumParams *params,
                      BufferAccessStrategy bstrategy) {
    LOCKMODE lmode;
    Relation rel;
    LockRelId lockrelid;
    Oid toast_relid;
    Oid save_userid;
    int save_sec_context;
    int save_nestlevel;
    VacuumParams toast_vacuum_params;

    // Copy parameters to avoid affecting TOAST processing
    memcpy(&toast_vacuum_params, params, sizeof(VacuumParams));

    // Start transaction for this vacuum operation
    StartTransactionCommand();

    // Set process flags for lazy vacuum coordination
    if (!(params->options & VACOPT_FULL)) {
        LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
        MyProc->statusFlags |= PROC_IN_VACUUM;
        if (params->is_wraparound)
            MyProc->statusFlags |= PROC_VACUUM_FOR_WRAPAROUND;
        ProcGlobal->statusFlags[MyProc->pgxactoff] = MyProc->statusFlags;
        LWLockRelease(ProcArrayLock);
    }

    // Acquire snapshot to prevent log truncation
    PushActiveSnapshot(GetTransactionSnapshot());
    CHECK_FOR_INTERRUPTS();

    // Determine lock mode and open relation
    lmode = (params->options & VACOPT_FULL) ?
            AccessExclusiveLock : ShareUpdateExclusiveLock;

    rel = vacuum_open_relation(relid, relation, params->options,
                              params->log_min_duration >= 0, lmode);
    if (!rel) {
        PopActiveSnapshot();
        CommitTransactionCommand();
        return false;
    }

    // Check privileges and relation validity
    Oid priv_relid = OidIsValid(params->toast_parent) ?
                     params->toast_parent : RelationGetRelid(rel);

    if (!vacuum_is_permitted_for_relation(priv_relid, rel->rd_rel,
                                         params->options & ~VACOPT_ANALYZE)) {
        relation_close(rel, lmode);
        PopActiveSnapshot();
        CommitTransactionCommand();
        return false;
    }

    // Check for valid relation types
    if (rel->rd_rel->relkind != RELKIND_RELATION &&
        rel->rd_rel->relkind != RELKIND_MATVIEW &&
        rel->rd_rel->relkind != RELKIND_TOASTVALUE &&
        rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE) {
        ereport(WARNING, (errmsg("skipping \"%s\" --- cannot vacuum non-tables",
                                RelationGetRelationName(rel))));
        relation_close(rel, lmode);
        PopActiveSnapshot();
        CommitTransactionCommand();
        return false;
    }

    // Skip temp tables of other backends and partitioned tables
    if (RELATION_IS_OTHER_TEMP(rel) ||
        rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) {
        relation_close(rel, lmode);
        PopActiveSnapshot();
        CommitTransactionCommand();
        return true;  // OK to proceed with ANALYZE for partitioned tables
    }

    // Acquire session-level lock for TOAST processing safety
    lockrelid = rel->rd_lockInfo.lockRelId;
    LockRelationIdForSession(&lockrelid, lmode);

    // Resolve vacuum options from relation settings if not specified
    if (params->index_cleanup == VACOPTVALUE_UNSPECIFIED) {
        // Set index_cleanup based on relation options or defaults
        params->index_cleanup = VACOPTVALUE_AUTO;  // Simplified logic
    }
    if (params->truncate == VACOPTVALUE_UNSPECIFIED) {
        // Set truncate based on relation options or defaults
        params->truncate = VACOPTVALUE_ENABLED;  // Simplified logic
    }

    // Remember TOAST relation for later processing
    if ((params->options & VACOPT_PROCESS_TOAST) != 0 &&
        ((params->options & VACOPT_FULL) == 0 ||
         (params->options & VACOPT_PROCESS_MAIN) == 0)) {
        toast_relid = rel->rd_rel->reltoastrelid;
    } else {
        toast_relid = InvalidOid;
    }

    // Switch to table owner's security context
    GetUserIdAndSecContext(&save_userid, &save_sec_context);
    SetUserIdAndSecContext(rel->rd_rel->relowner,
                          save_sec_context | SECURITY_RESTRICTED_OPERATION);
    save_nestlevel = NewGUCNestLevel();
    RestrictSearchPath();

    // Perform the actual vacuum work if PROCESS_MAIN is set
    if (params->options & VACOPT_PROCESS_MAIN) {
        if (params->options & VACOPT_FULL) {
            // VACUUM FULL uses cluster_rel
            ClusterParams cluster_params = {0};
            relation_close(rel, NoLock);
            rel = NULL;
            if (params->options & VACOPT_VERBOSE)
                cluster_params.options |= CLUOPT_VERBOSE;
            cluster_rel(relid, InvalidOid, &cluster_params);
        } else {
            // Lazy vacuum
            table_relation_vacuum(rel, params, bstrategy);
        }
    }

    // Restore security context and complete transaction
    AtEOXact_GUC(false, save_nestlevel);
    SetUserIdAndSecContext(save_userid, save_sec_context);
    if (rel)
        relation_close(rel, NoLock);
    PopActiveSnapshot();
    CommitTransactionCommand();

    // Process TOAST table if needed
    if (toast_relid != InvalidOid) {
        toast_vacuum_params.options |= VACOPT_PROCESS_MAIN;
        toast_vacuum_params.toast_parent = relid;
        vacuum_rel(toast_relid, NULL, &toast_vacuum_params, bstrategy);
    }

    // Release session lock
    UnlockRelationIdForSession(&lockrelid, lmode);
    return true;
}
```
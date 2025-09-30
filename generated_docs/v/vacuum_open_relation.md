# vacuum_open_relation

## Location
[src/backend/commands/vacuum.c:769-880](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L769-L880)

## Overview
Attempts to open and lock a relation for vacuum or analyze operations, providing comprehensive error handling and logging when the relation cannot be accessed.

## Definition

```c
Relation
vacuum_open_relation(Oid relid, RangeVar *relation, bits32 options,
					 bool verbose, LOCKMODE lmode)
```
## Detailed Description
This function serves as a robust wrapper for opening relations that need to be vacuumed or analyzed. It handles the complexities of relation locking, including optional non-blocking lock acquisition and comprehensive error reporting when relations cannot be accessed.

The function implements sophisticated locking logic based on the VACOPT_SKIP_LOCKED option:
- Normal mode: Uses try_relation_open() directly with the requested lock mode
- Skip-locked mode: First attempts conditional locking, then opens with NoLock if successful

When relation access fails, the function provides detailed diagnostic information through appropriate log levels (WARNING for manual operations, LOG for autovacuum when verbose). It distinguishes between two failure scenarios: lock unavailability and relation non-existence, providing specific error codes and messages for each case.

The function handles race conditions gracefully - relations may disappear between discovery and processing, which is treated as a normal condition rather than an error.

## Parameters / Member Variables
- : Object identifier (OID) of the relation to open
- : RangeVar structure containing relation name information for error reporting (may be NULL)
- : Bitfield specifying the operation type (VACOPT_VACUUM, VACOPT_ANALYZE) and behavioral flags like VACOPT_SKIP_LOCKED
- : Boolean flag indicating whether verbose logging should be performed
- : Lock mode to acquire on the relation (e.g., ShareUpdateExclusiveLock for vacuum)

## Dependencies
- Functions called/Symbols referenced:
  - [try_relation_open](../t/try_relation_open.md) (safe relation opening)
  - [ConditionalLockRelationOid](../C/ConditionalLockRelationOid.md) (non-blocking lock acquisition)
  - AmAutoVacuumWorkerProcess (process type detection)
  - ereport (error/warning reporting)
  - ERRCODE_LOCK_NOT_AVAILABLE, ERRCODE_UNDEFINED_TABLE (error codes)
- Called from (representative examples):
  - [vacuum_rel](vacuum_rel.md) (main vacuum processing)
  - [analyze_rel](../a/analyze_rel.md) (analysis processing)

## Notes and Other Information
- Returns opened Relation on success, NULL on failure
- Implements different logging strategies for manual vs. autovacuum operations (WARNING vs. LOG level)
- For VACUUM ANALYZE operations, only logs VACUUM-related messages to avoid duplicate reporting
- Handles the SKIP_LOCKED option by attempting conditional locking before relation opening
- Gracefully handles relations that disappear during processing (common in high-concurrency environments)
- When RangeVar is NULL, skips detailed error reporting (allows callers to suppress logging intentionally)
- Error messages include specific relation names and distinguish between lock contention and missing relations

## Simplified Source

```c
Relation
vacuum_open_relation(Oid relid, RangeVar *relation, bits32 options,
                     bool verbose, LOCKMODE lmode)
{
    Relation rel;
    bool rel_lock = true;
    int elevel;

    Assert((options & (VACOPT_VACUUM | VACOPT_ANALYZE)) != 0);

    // Try to open relation with appropriate locking strategy
    if (!(options & VACOPT_SKIP_LOCKED))
        rel = try_relation_open(relid, lmode);
    else if (ConditionalLockRelationOid(relid, lmode))
        rel = try_relation_open(relid, NoLock);
    else
    {
        rel = NULL;
        rel_lock = false;
    }

    // Success - return opened relation
    if (rel)
        return rel;

    // Skip logging if no RangeVar provided
    if (relation == NULL)
        return NULL;

    // Determine appropriate log level
    if (!AmAutoVacuumWorkerProcess())
        elevel = WARNING;
    else if (verbose)
        elevel = LOG;
    else
        return NULL;

    // Log appropriate error message
    if ((options & VACOPT_VACUUM) != 0)
    {
        if (!rel_lock)
            ereport(elevel, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                    errmsg("skipping vacuum of \"%s\" --- lock not available",
                           relation->relname)));
        else
            ereport(elevel, (errcode(ERRCODE_UNDEFINED_TABLE),
                    errmsg("skipping vacuum of \"%s\" --- relation no longer exists",
                           relation->relname)));
        return NULL;
    }

    if ((options & VACOPT_ANALYZE) != 0)
    {
        if (!rel_lock)
            ereport(elevel, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                    errmsg("skipping analyze of \"%s\" --- lock not available",
                           relation->relname)));
        else
            ereport(elevel, (errcode(ERRCODE_UNDEFINED_TABLE),
                    errmsg("skipping analyze of \"%s\" --- relation no longer exists",
                           relation->relname)));
    }

    return NULL;
}
```
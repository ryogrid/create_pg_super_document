# GetNewMultiXactId

## Location
[src/backend/access/transam/multixact.c:1026-1178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L1026-L1178)

## Overview
Assigns a new MultiXactId and reserves the required space in the members area, with comprehensive wraparound protection and autovacuum triggering mechanisms.

## Definition
```c
static MultiXactId GetNewMultiXactId(int nmembers, MultiXactOffset *offset)
```

## Detailed Description
GetNewMultiXactId is a critical static function that manages the allocation of new MultiXact IDs while ensuring system safety and preventing data loss from wraparound conditions. The function performs multiple layers of protection: it checks various limits (vacuum, warning, and stop limits) to prevent MultiXact ID wraparound, triggers autovacuum when necessary, and manages both MultiXact ID and member offset allocation.

The function implements sophisticated wraparound protection by monitoring several thresholds and taking appropriate action when limits are approached. It can issue warnings, trigger autovacuum processes, or refuse to allocate new IDs when safety limits are exceeded. The function also ensures proper space allocation in SLRU files and maintains critical sections to ensure atomicity of counter updates.

## Parameters / Member Variables
- `nmembers`: Number of transaction members that will be stored for this MultiXact
- `offset`: Pointer to MultiXactOffset variable that receives the starting offset in the members file where this MultiXact's members will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md) (recovery state checking)
  - [LWLockAcquire](../L/LWLockAcquire.md), LWLockRelease (with MultiXactGenLock)
  - [MultiXactIdPrecedes](../M/MultiXactIdPrecedes.md) (wraparound-aware comparison)
  - [SendPostmasterSignal](../S/SendPostmasterSignal.md) (autovacuum triggering)
  - [get_database_name](../g/get_database_name.md) (database name lookup)
  - ereport, errmsg_plural (error/warning reporting)
  - [ExtendMultiXactOffset](../E/ExtendMultiXactOffset.md), ExtendMultiXactMember (SLRU file extension)
  - [MultiXactOffsetWouldWrap](../M/MultiXactOffsetWouldWrap.md) (wraparound checking)
  - START_CRIT_SECTION (critical section management)
  - debug_elog3, debug_elog4 (debugging)
- Called from (representative examples):
  - [MultiXactIdCreateFromMembers](../M/MultiXactIdCreateFromMembers.md) (during MultiXact creation)

## Notes and Other Information
- Function is marked static and used internally within the MultiXact subsystem
- Implements comprehensive wraparound protection with multiple threshold levels
- Automatically triggers autovacuum when approaching dangerous conditions
- Uses critical sections to ensure atomic updates of shared counters
- Handles recovery scenarios by preventing MultiXact assignment during recovery
- Manages both MultiXact ID assignment and member space reservation
- Implements safety checks to prevent catastrophic data loss
- Returns offset 1 instead of 0 to avoid issues with invalid offset values
- The caller must end the critical section after writing SLRU data

## Simplified Source

```c
static MultiXactId
GetNewMultiXactId(int nmembers, MultiXactOffset *offset)
{
    MultiXactId result;
    MultiXactOffset nextOffset;

    debug_elog3(DEBUG2, "GetNew: for %d xids", nmembers);

    // Safety check: no MultiXact assignment during recovery
    if (RecoveryInProgress())
        elog(ERROR, "cannot assign MultiXactIds during recovery");

    LWLockAcquire(MultiXactGenLock, LW_EXCLUSIVE);

    // Handle counter wraparound
    if (MultiXactState->nextMXact < FirstMultiXactId)
        MultiXactState->nextMXact = FirstMultiXactId;

    result = MultiXactState->nextMXact;

    // Comprehensive wraparound protection checks
    if (!MultiXactIdPrecedes(result, MultiXactState->multiVacLimit))
    {
        // Copy shared values before releasing lock
        MultiXactId multiWarnLimit = MultiXactState->multiWarnLimit;
        MultiXactId multiStopLimit = MultiXactState->multiStopLimit;
        MultiXactId multiWrapLimit = MultiXactState->multiWrapLimit;
        Oid oldest_datoid = MultiXactState->oldestMultiXactDB;

        LWLockRelease(MultiXactGenLock);

        // STOP limit check - refuse new assignments
        if (IsUnderPostmaster && !MultiXactIdPrecedes(result, multiStopLimit))
        {
            char *oldest_datname = get_database_name(oldest_datoid);
            SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);

            if (oldest_datname)
                ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                                errmsg("database is not accepting commands that assign new MultiXactIds to avoid wraparound data loss in database \"%s\"", oldest_datname),
                                errhint("Execute a database-wide VACUUM in that database.")));
            else
                ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                                errmsg("database is not accepting commands that assign new MultiXactIds to avoid wraparound data loss in database with OID %u", oldest_datoid),
                                errhint("Execute a database-wide VACUUM in that database.")));
        }

        // Trigger autovacuum periodically
        if (IsUnderPostmaster && (result % 65536) == 0)
            SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);

        // WARNING limit check
        if (!MultiXactIdPrecedes(result, multiWarnLimit))
        {
            char *oldest_datname = get_database_name(oldest_datoid);
            if (oldest_datname)
                ereport(WARNING, (errmsg_plural("database \"%s\" must be vacuumed before %u more MultiXactId is used",
                                                "database \"%s\" must be vacuumed before %u more MultiXactIds are used",
                                                multiWrapLimit - result, oldest_datname, multiWrapLimit - result),
                                  errhint("Execute a database-wide VACUUM in that database.")));
            else
                ereport(WARNING, (errmsg_plural("database with OID %u must be vacuumed before %u more MultiXactId is used",
                                                "database with OID %u must be vacuumed before %u more MultiXactIds are used",
                                                multiWrapLimit - result, oldest_datoid, multiWrapLimit - result),
                                  errhint("Execute a database-wide VACUUM in that database.")));
        }

        // Re-acquire lock and refresh result
        LWLockAcquire(MultiXactGenLock, LW_EXCLUSIVE);
        result = MultiXactState->nextMXact;
        if (result < FirstMultiXactId)
            result = FirstMultiXactId;
    }

    // Ensure SLRU file space for the new MultiXactId
    ExtendMultiXactOffset(result);

    // Reserve member space (avoid returning offset 0)
    nextOffset = MultiXactState->nextOffset;
    if (nextOffset == 0)
    {
        *offset = 1;
        nmembers++;  // Allocate member slot 0 too
    }
    else
        *offset = nextOffset;

    // Check for member space wraparound
    if (MultiXactOffsetWouldWrap(MultiXactState->nextOffset, nmembers))
    {
        LWLockRelease(MultiXactGenLock);
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("cannot acquire additional MultiXactId locks now"),
                        errhint("Retry after committing or rolling back the transaction.")));
    }

    // Ensure SLRU file space for members
    ExtendMultiXactMember(nextOffset, nmembers);

    // Start critical section before updating shared counters
    START_CRIT_SECTION();

    // Update counters atomically
    MultiXactState->nextMXact = result + 1;
    MultiXactState->nextOffset += nmembers;

    debug_elog4(DEBUG2, "GetNew: assigned %u offset %u", result, *offset);

    LWLockRelease(MultiXactGenLock);

    return result;
}
```
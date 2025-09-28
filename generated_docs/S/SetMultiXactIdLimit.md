# SetMultiXactIdLimit

## Location
[src/backend/access/transam/multixact.c:2354-2502](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L2354-L2502)

## Overview
Determines and sets the safe limits for MultiXact ID allocation based on the oldest datminmxid to prevent wraparound and data loss.

## Definition

```c
void
SetMultiXactIdLimit(MultiXactId oldest_datminmxid, Oid oldest_datoid,
					bool is_startup)
```
## Detailed Description
SetMultiXactIdLimit is a critical function for preventing MultiXact ID wraparound by calculating and setting various safety limits based on the oldest MultiXact ID that might exist in any database of the cluster. The function establishes a multi-tiered warning and protection system similar to transaction ID wraparound protection.

The function calculates four key limits:
1. **multiWrapLimit**: The theoretical wrap point (oldest + MaxMultiXactId/2)
2. **multiStopLimit**: Hard stop limit (3M before wrap) - refuses new MultiXact assignments
3. **multiWarnLimit**: Warning limit (40M before wrap) - issues warnings to administrators
4. **multiVacLimit**: Vacuum trigger limit (based on autovacuum_multixact_freeze_max_age)

The function also handles offset vacuum limits separately and can trigger autovacuum processes when limits are approached. It includes comprehensive error reporting with database-specific messages to help administrators identify which databases need attention.

## Parameters / Member Variables
- : The oldest MultiXact ID that might exist in any database of the cluster
- : The OID of the database containing the oldest MultiXact ID
- : Boolean indicating whether this is called during startup (affects logging only)

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactIdIsValid
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - ereport
  - [SetOffsetVacuumLimit](SetOffsetVacuumLimit.md)
  - [MultiXactIdPrecedes](../M/MultiXactIdPrecedes.md)
  - [SendPostmasterSignal](SendPostmasterSignal.md)
  - [IsTransactionState](../I/IsTransactionState.md)
  - [get_database_name](../g/get_database_name.md)
  - [errmsg_plural](../e/errmsg_plural.md)
- Called from (representative examples):
  - [TrimMultiXact](../T/TrimMultiXact.md)
  - [MultiXactAdvanceOldest](../M/MultiXactAdvanceOldest.md)
  - [multixact_redo](../m/multixact_redo.md)
  - [BootStrapXLOG](../B/BootStrapXLOG.md)
  - [StartupXLOG](StartupXLOG.md)
  - [vac_truncate_clog](../v/vac_truncate_clog.md)

## Notes and Other Information
- Critical for preventing MultiXact wraparound and data loss
- Establishes a four-tier warning system (vacuum, warn, stop, wrap limits)
- Only performs full limit calculations after TrimMultiXact() has completed startup
- Automatically triggers autovacuum launcher when limits are approached
- Provides detailed warning messages with specific databases and remediation steps
- Uses exclusive lock on MultiXactGenLock for atomic limit updates
- The 40M warning threshold is intentionally not configurable to prevent misconfiguration
- Handles both transaction and non-transaction contexts for database name resolution
- Works in conjunction with SetOffsetVacuumLimit for comprehensive MultiXact management

## Simplified Source

```c
// Simplified version of SetMultiXactIdLimit
void SetMultiXactIdLimit(MultiXactId oldest_datminmxid, Oid oldest_datoid, bool is_startup) {
    MultiXactId multiVacLimit, multiWarnLimit, multiStopLimit, multiWrapLimit;
    MultiXactId curMulti;
    bool needs_offset_vacuum;

    // Calculate wrap limit - halfway through multixact ID space
    multiWrapLimit = oldest_datminmxid + (MaxMultiXactId >> 1);
    if (multiWrapLimit < FirstMultiXactId)
        multiWrapLimit += FirstMultiXactId;

    // Set hard stop limit - refuse assignments 3M before wrap
    multiStopLimit = multiWrapLimit - 3000000;
    if (multiStopLimit < FirstMultiXactId)
        multiStopLimit -= FirstMultiXactId;

    // Set warning limit - start complaining 40M before wrap
    multiWarnLimit = multiWrapLimit - 40000000;
    if (multiWarnLimit < FirstMultiXactId)
        multiWarnLimit -= FirstMultiXactId;

    // Set vacuum trigger limit based on freeze max age
    multiVacLimit = oldest_datminmxid + autovacuum_multixact_freeze_max_age;
    if (multiVacLimit < FirstMultiXactId)
        multiVacLimit += FirstMultiXactId;

    // Atomically update all limits in shared state
    LWLockAcquire(MultiXactGenLock, LW_EXCLUSIVE);
    MultiXactState->oldestMultiXactId = oldest_datminmxid;
    MultiXactState->oldestMultiXactDB = oldest_datoid;
    MultiXactState->multiVacLimit = multiVacLimit;
    MultiXactState->multiWarnLimit = multiWarnLimit;
    MultiXactState->multiStopLimit = multiStopLimit;
    MultiXactState->multiWrapLimit = multiWrapLimit;
    curMulti = MultiXactState->nextMXact;
    LWLockRelease(MultiXactGenLock);

    // Skip remaining checks if still in startup/recovery
    if (!MultiXactState->finishedStartup)
        return;

    // Set offset vacuum limits and check if offset vacuum needed
    needs_offset_vacuum = SetOffsetVacuumLimit(is_startup);

    // Trigger autovacuum if past vacuum limit or offset vacuum needed
    if ((MultiXactIdPrecedes(multiVacLimit, curMulti) || needs_offset_vacuum) && IsUnderPostmaster)
        SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);

    // Issue warning if past warning limit
    if (MultiXactIdPrecedes(multiWarnLimit, curMulti)) {
        char *oldest_datname = IsTransactionState() ? get_database_name(oldest_datoid) : NULL;

        if (oldest_datname) {
            ereport(WARNING, (errmsg_plural(
                "database \"%s\" must be vacuumed before %u more MultiXactId is used",
                "database \"%s\" must be vacuumed before %u more MultiXactIds are used",
                multiWrapLimit - curMulti, oldest_datname, multiWrapLimit - curMulti)));
        } else {
            ereport(WARNING, (errmsg_plural(
                "database with OID %u must be vacuumed before %u more MultiXactId is used",
                "database with OID %u must be vacuumed before %u more MultiXactIds are used",
                multiWrapLimit - curMulti, oldest_datoid, multiWrapLimit - curMulti)));
        }
    }
}
```

Key simplifications made:
- Removed detailed comments and consolidated variable declarations
- Simplified the wrap limit calculation logic while preserving the wraparound handling
- Consolidated the four limit calculations into a cleaner sequence
- Removed debug logging for clarity
- Simplified the warning message handling by combining both cases
- Removed detailed error hints to focus on core warning logic
- Abstracted the complex plural message formatting while keeping the essential warning
- Maintained all critical functionality: limit calculation, atomic state updates, autovacuum triggering, and warning generation
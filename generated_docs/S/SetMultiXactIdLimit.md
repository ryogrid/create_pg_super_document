# SetMultiXactIdLimit

## Location
[src/backend/access/transam/multixact.c:2354-2502](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L2354-L2502)

## Overview
Determines and sets the safe limits for MultiXact ID allocation based on the oldest datminmxid to prevent wraparound and data loss.

## Definition


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
  - LWLockAcquire
  - LWLockRelease
  - ereport
  - [SetOffsetVacuumLimit](SetOffsetVacuumLimit.md)
  - [MultiXactIdPrecedes](../M/MultiXactIdPrecedes.md)
  - SendPostmasterSignal
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
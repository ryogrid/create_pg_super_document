# CheckRequiredParameterValues

## Location
[src/backend/access/transam/xlog.c:5340-5383](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L5340-L5383)

## Overview
Validates that server configuration parameters are set to appropriate values for archive recovery and hot standby operations to prevent recovery failures.

## Definition

```c
static void
CheckRequiredParameterValues(void)
```
## Detailed Description
CheckRequiredParameterValues performs critical validation of server configuration parameters to ensure compatibility with recovery operations. This function implements essential safety checks that prevent runtime failures during archive recovery and hot standby scenarios.

The function performs two primary categories of validation:

1. **WAL Level Validation**: Ensures that the WAL was generated with at least 'replica' wal_level when archive recovery is requested. WAL generated with 'minimal' level lacks the necessary information for recovery operations.

2. **Hot Standby Resource Validation**: When Hot Standby is enabled during archive recovery, validates that the standby server has sufficient resources allocated compared to the primary server. This includes checking:
   - max_connections: Ensures adequate connection slots
   - max_worker_processes: Validates worker process capacity
   - max_wal_senders: Checks WAL sender allocation
   - max_prepared_transactions: Verifies prepared transaction support
   - max_locks_per_transaction: Ensures sufficient lock table capacity

These validations are crucial because insufficient resources or incompatible WAL levels would cause recovery to fail at runtime, potentially after significant time investment. By checking these conditions early, the function provides clear error messages and prevents wasted recovery attempts.

The function references the Administrator's Overview section in high-availability.sgml, indicating that these parameters are documented as requirements for high-availability configurations.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - WAL_LEVEL_MINIMAL: Constant representing minimal WAL logging level
  - [RecoveryRequiresIntParameter](../R/RecoveryRequiresIntParameter.md): Helper function that validates integer parameter values between current server and control file
  - ControlFile: Global structure containing cluster configuration metadata
  - ArchiveRecoveryRequested: Global flag indicating archive recovery mode
  - EnableHotStandby: Global flag indicating hot standby mode

- Called from (representative examples):
  - [StartupXLOG](../S/StartupXLOG.md): Called during recovery startup to validate configuration
  - RefreshXLogWriteResult: Called when updating WAL write results
  - [xlog_redo](../x/xlog_redo.md): Called during WAL record replay operations

## Notes and Other Information
- This is a static function internal to the xlog.c module
- Designed to fail fast with clear error messages rather than allowing recovery to proceed with insufficient resources
- The function explicitly excludes autovacuum_max_workers from the max_connections validation
- All parameter checks reference both current server settings and values stored in the control file from the primary
- Critical for preventing resource exhaustion during hot standby operations
- Error messages provide specific guidance on how to resolve configuration issues
- The validation ensures that standby servers can handle the same workload characteristics as the primary server

## Simplified Source

```c
// Simplified version of CheckRequiredParameterValues
static void
CheckRequiredParameterValues(void)
{
    // Check 1: Verify WAL level for archive recovery
    if (ArchiveRecoveryRequested && ControlFile->wal_level == WAL_LEVEL_MINIMAL) {
        ereport(FATAL,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("WAL was generated with \"wal_level=minimal\", cannot continue recovering"),
                 errhint("Use a backup taken after setting \"wal_level\" to higher than \"minimal\".")));
    }

    // Check 2: Verify resource parameters for Hot Standby
    if (ArchiveRecoveryRequested && EnableHotStandby) {
        // Ensure standby has adequate resources compared to primary
        RecoveryRequiresIntParameter("max_connections",
                                   MaxConnections, ControlFile->MaxConnections);
        RecoveryRequiresIntParameter("max_worker_processes",
                                   max_worker_processes, ControlFile->max_worker_processes);
        RecoveryRequiresIntParameter("max_wal_senders",
                                   max_wal_senders, ControlFile->max_wal_senders);
        RecoveryRequiresIntParameter("max_prepared_transactions",
                                   max_prepared_xacts, ControlFile->max_prepared_xacts);
        RecoveryRequiresIntParameter("max_locks_per_transaction",
                                   max_locks_per_xact, ControlFile->max_locks_per_xact);
    }
}
```

Key simplifications made:
- Condensed detailed error messages while preserving essential information
- Removed verbose error details and combined related error fields
- Added clear comments explaining the two main validation categories
- Simplified variable names in comments for better readability
- Maintained the core logic flow and all essential parameter checks
- Preserved the fatal error reporting for WAL level mismatch
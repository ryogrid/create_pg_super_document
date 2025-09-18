# CheckRequiredParameterValues

## Location
src/backend/access/transam/xlog.c: 5340 - 5383

## Overview
Validates that server configuration parameters are set to appropriate values for archive recovery and hot standby operations to prevent recovery failures.

## Definition


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
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - WAL_LEVEL_MINIMAL: Constant representing minimal WAL logging level
  - RecoveryRequiresIntParameter: Helper function that validates integer parameter values between current server and control file
  - ControlFile: Global structure containing cluster configuration metadata
  - ArchiveRecoveryRequested: Global flag indicating archive recovery mode
  - EnableHotStandby: Global flag indicating hot standby mode

- Called from (representative examples):
  - StartupXLOG: Called during recovery startup to validate configuration
  - RefreshXLogWriteResult: Called when updating WAL write results
  - xlog_redo: Called during WAL record replay operations

## Notes and Other Information
- This is a static function internal to the xlog.c module
- Designed to fail fast with clear error messages rather than allowing recovery to proceed with insufficient resources
- The function explicitly excludes autovacuum_max_workers from the max_connections validation
- All parameter checks reference both current server settings and values stored in the control file from the primary
- Critical for preventing resource exhaustion during hot standby operations
- Error messages provide specific guidance on how to resolve configuration issues
- The validation ensures that standby servers can handle the same workload characteristics as the primary server
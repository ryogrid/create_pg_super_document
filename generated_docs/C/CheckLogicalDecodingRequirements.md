# CheckLogicalDecodingRequirements

## Location
src/backend/replication/logical/logical.c: 111 - 151

## Overview
CheckLogicalDecodingRequirements validates that the current PostgreSQL environment and configuration settings are capable of performing logical decoding operations.

## Definition


## Detailed Description
This function performs essential prerequisite validation before logical decoding can be initiated. It ensures that all necessary conditions are met for logical replication to function correctly, including proper WAL level configuration, database connection requirements, and standby-specific constraints.

The function performs multiple validation checks in sequence:
1. Calls CheckSlotRequirements() to verify slot-related prerequisites
2. Validates that wal_level is set to at least WAL_LEVEL_LOGICAL
3. Ensures a valid database connection exists (MyDatabaseId != InvalidOid)
4. For standby servers, verifies that the primary server has appropriate wal_level settings

The function includes race condition handling for standby scenarios, where wal_level changes are verified through XLOG_PARAMETER_CHANGE records, and the function is called both before slot creation and during logical decoding startup.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - CheckSlotRequirements: Validates replication slot prerequisites
  - RecoveryInProgress: Checks if the server is in recovery mode
  - GetActiveWalLevelOnStandby: Gets WAL level from standby server
  - WAL_LEVEL_LOGICAL: Constant defining minimum WAL level for logical decoding

- Called from (representative examples):
  - CreateInitDecodingContext: During initial decoding context creation
  - pg_logical_slot_get_changes_guts: Before retrieving logical changes
  - CreateReplicationSlot: During replication slot creation
  - StartLogicalReplication: When starting logical replication

## Notes and Other Information
- The function includes a comment noting that adding new requirements may necessitate updates to RestoreSlotFromDisk()
- Race conditions are acknowledged for standby scenarios but are mitigated through careful sequencing of checks
- Errors thrown use ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE to indicate configuration issues
- Critical for ensuring logical replication reliability and preventing runtime failures
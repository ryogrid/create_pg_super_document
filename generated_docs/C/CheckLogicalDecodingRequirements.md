# CheckLogicalDecodingRequirements

## Location
[src/backend/replication/logical/logical.c:111-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L111-L151)

## Overview
CheckLogicalDecodingRequirements validates that the current PostgreSQL environment and configuration settings are capable of performing logical decoding operations.

## Definition

```c
void
CheckLogicalDecodingRequirements(void)
```
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
  - [CheckSlotRequirements](CheckSlotRequirements.md): Validates replication slot prerequisites
  - [RecoveryInProgress](../R/RecoveryInProgress.md): Checks if the server is in recovery mode
  - [GetActiveWalLevelOnStandby](../G/GetActiveWalLevelOnStandby.md): Gets WAL level from standby server
  - WAL_LEVEL_LOGICAL: Constant defining minimum WAL level for logical decoding

- Called from (representative examples):
  - [CreateInitDecodingContext](CreateInitDecodingContext.md): During initial decoding context creation
  - [pg_logical_slot_get_changes_guts](../p/pg_logical_slot_get_changes_guts.md): Before retrieving logical changes
  - [CreateReplicationSlot](CreateReplicationSlot.md): During replication slot creation
  - [StartLogicalReplication](../S/StartLogicalReplication.md): When starting logical replication

## Notes and Other Information
- The function includes a comment noting that adding new requirements may necessitate updates to RestoreSlotFromDisk()
- Race conditions are acknowledged for standby scenarios but are mitigated through careful sequencing of checks
- Errors thrown use ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE to indicate configuration issues
- Critical for ensuring logical replication reliability and preventing runtime failures

## Simplified Source

```c
// Simplified version of CheckLogicalDecodingRequirements
void CheckLogicalDecodingRequirements(void) {
    // Check basic slot requirements
    CheckSlotRequirements();

    // Verify minimum WAL level for logical decoding
    if (wal_level < WAL_LEVEL_LOGICAL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("logical decoding requires \"wal_level\" >= \"logical\"")));

    // Ensure valid database connection
    if (MyDatabaseId == InvalidOid)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("logical decoding requires a database connection")));

    // Additional checks for standby servers
    if (RecoveryInProgress()) {
        // Verify primary server has adequate wal_level
        if (GetActiveWalLevelOnStandby() < WAL_LEVEL_LOGICAL)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                           errmsg("logical decoding on standby requires \"wal_level\" >= \"logical\" on the primary")));
    }
}
```

Key simplifications made:
- Added clear comments explaining each validation step
- Grouped related checks logically
- Simplified conditional structure while preserving all essential validations
- Maintained detailed error messages for diagnosis
- Preserved all critical prerequisite checks for logical decoding
# ActivateCommitTs

## Location
src/backend/access/transam/commit_ts.c: 705 - 784

## Overview
ActivateCommitTs enables the commit timestamp subsystem by initializing shared memory state, creating necessary SLRU segments, and setting up transaction ID boundaries for commit timestamp tracking.

## Definition
```c
static void ActivateCommitTs(void)
```

## Detailed Description
This function performs the complete activation of the commit timestamp subsystem, handling both initial startup scenarios and runtime activation during WAL replay. Unlike other SLRU subsystems that are simply initialized during startup, the commit timestamp system requires special activation/deactivation logic because it can be dynamically enabled or disabled and these changes must be propagated from primary to standby servers.

The function performs several key operations:
1. Checks if the subsystem is already active to avoid redundant activation
2. Calculates the current page number based on the next transaction ID
3. Initializes the latest page number in shared memory
4. Sets up transaction ID boundaries (oldest and newest commit timestamp XIDs) if not already established
5. Creates the current SLRU segment file if it doesn't exist
6. Marks the subsystem as active in shared memory

The function includes special handling for cases where the server was previously running with commit timestamps disabled, ensuring proper initialization of data structures.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode
  - XidFromFullTransactionId
  - [TransactionIdToCTsPage](../T/TransactionIdToCTsPage.md)
  - [pg_atomic_write_u64](../p/pg_atomic_write_u64.md)
  - ReadNextTransactionId
  - [SimpleLruDoesPhysicalPageExist](../S/SimpleLruDoesPhysicalPageExist.md)
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md)
  - [ZeroCommitTsPage](../Z/ZeroCommitTsPage.md)
  - [SimpleLruWritePage](../S/SimpleLruWritePage.md)
  - CommitTsCtl
  - TransamVariables
  - commitTsShared
- Called from (representative examples):
  - [StartupCommitTs](../S/StartupCommitTs.md)
  - [CompleteCommitTsInitialization](../C/CompleteCommitTsInitialization.md)
  - [CommitTsParameterChange](../C/CommitTsParameterChange.md)

## Notes and Other Information
- This is a static function, only accessible within the commit_ts.c module
- Skips activation during bootstrap processing mode to avoid unnecessary overhead
- Uses proper locking with CommitTsLock to ensure thread-safe activation
- Handles edge cases where the server was previously running with commit timestamps disabled
- Creates physical SLRU pages on demand if they don't exist
- Updates both the SLRU control structure and the commit timestamp shared memory state
- The function is idempotent - calling it multiple times when already active is safe
- Critical for maintaining consistency between primary and standby servers in replication scenarios
- Includes detailed comments explaining potential issues with enable/disable/re-enable scenarios
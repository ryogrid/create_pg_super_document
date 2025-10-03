# ActivateCommitTs

## Location
[src/backend/access/transam/commit_ts.c:705-784](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L705-L784)

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

## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode
  - XidFromFullTransactionId
  - [TransactionIdToCTsPage](../T/TransactionIdToCTsPage.md)
  - [pg_atomic_write_u64](../p/pg_atomic_write_u64.md)
  - [ReadNextTransactionId](../R/ReadNextTransactionId.md)
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

## Simplified Source

```c
// Simplified version of ActivateCommitTs
static void
ActivateCommitTs(void)
{
    TransactionId xid;
    int64 pageno;

    // Skip activation during bootstrap mode
    if (IsBootstrapProcessingMode())
        return;

    // Check if already active - early return if so
    LWLockAcquire(CommitTsLock, LW_EXCLUSIVE);
    if (commitTsShared->commitTsActive) {
        LWLockRelease(CommitTsLock);
        return;
    }
    LWLockRelease(CommitTsLock);

    // Calculate current page number from next transaction ID
    xid = XidFromFullTransactionId(TransamVariables->nextXid);
    pageno = TransactionIdToCTsPage(xid);

    // Initialize latest page number in shared memory
    pg_atomic_write_u64(&CommitTsCtl->shared->latest_page_number, pageno);

    // Set up transaction ID boundaries for commit timestamp tracking
    LWLockAcquire(CommitTsLock, LW_EXCLUSIVE);
    if (TransamVariables->oldestCommitTsXid == InvalidTransactionId) {
        TransamVariables->oldestCommitTsXid =
            TransamVariables->newestCommitTsXid = ReadNextTransactionId();
    }
    LWLockRelease(CommitTsLock);

    // Create the current SLRU segment file if needed
    if (!SimpleLruDoesPhysicalPageExist(CommitTsCtl, pageno)) {
        LWLock *lock = SimpleLruGetBankLock(CommitTsCtl, pageno);
        int slotno;

        LWLockAcquire(lock, LW_EXCLUSIVE);
        slotno = ZeroCommitTsPage(pageno, false);
        SimpleLruWritePage(CommitTsCtl, slotno);
        LWLockRelease(lock);
    }

    // Mark subsystem as active in shared memory
    LWLockAcquire(CommitTsLock, LW_EXCLUSIVE);
    commitTsShared->commitTsActive = true;
    LWLockRelease(CommitTsLock);
}
```

Key simplifications made:
- Removed detailed comments explaining edge cases and XXX concerns
- Simplified variable declarations and formatting
- Removed Assert statement for cleaner flow
- Consolidated lock operations with clearer grouping
- Streamlined conditional logic while preserving essential checks
- Maintained all critical functionality and error handling
- Preserved the core algorithm: check activation status, initialize page tracking, set boundaries, create segments, activate
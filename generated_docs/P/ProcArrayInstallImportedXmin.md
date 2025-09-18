# ProcArrayInstallImportedXmin

## Location
src/backend/storage/ipc/procarray.c: 2536 - 2619

## Overview
Installs an imported xmin from another transaction into the current backend's MyProc->xmin, ensuring OldestXmin cannot go backwards by verifying the source transaction is still running.

## Definition


## Detailed Description
ProcArrayInstallImportedXmin is used when importing a snapshot from another transaction, typically during snapshot sharing operations. The function ensures the integrity of the global transaction visibility horizon by verifying that the source transaction that created the snapshot is still active before installing its xmin value.

The function performs several critical safety checks:
1. **Source transaction verification**: Locates the source transaction by its virtual transaction ID (procNumber + localTransactionId)
2. **Database consistency**: Ensures the source transaction is in the same database
3. **Xmin validity**: Confirms the source transaction's xmin actually covers the requested xmin value
4. **Transaction state**: Verifies the source transaction is still running and not a VACUUM process

If all checks pass, the function atomically installs the imported xmin into both MyProc->xmin and TransactionXmin, maintaining consistency with the global transaction state. This prevents the system's oldest active transaction mark (OldestXmin) from moving backwards, which could lead to visibility inconsistencies.

## Parameters / Member Variables
- : The transaction ID to install as the new xmin value
- : Pointer to the VirtualTransactionId of the source transaction that created the snapshot being imported

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsNormal (validates transaction ID format)
  - TransactionIdPrecedesOrEquals (checks xmin coverage)
  - UINT32_ACCESS_ONCE (atomic read of transaction ID)
  - LWLockAcquire/LWLockRelease (ProcArrayLock protection)
- Called from (representative examples):
  - SetTransactionSnapshot (snapshot import operations)
  - GetSerializableTransactionSnapshotInt (serializable isolation support)

## Notes and Other Information
- Returns true if the import was successful, false if the source transaction is no longer running
- Requires ProcArrayLock in shared mode to ensure atomicity with transaction completion
- The function ignores VACUUM processes when searching for the source transaction
- Database ID checking prevents cross-database snapshot sharing which could cause visibility errors
- Used primarily in snapshot import scenarios where one transaction wants to adopt another's visibility rules
- Essential for maintaining MVCC consistency when sharing snapshots between transactions
- The installed xmin becomes the new lower bound for tuple visibility in the importing transaction
# ProcArrayInstallRestoredXmin

## Location
src/backend/storage/ipc/procarray.c: 2620 - 2692

## Overview
Installs a restored xmin from a specific PGPROC structure into the current backend's xmin, copying relevant status flags to maintain proper vacuum behavior and transaction visibility.

## Definition


## Detailed Description
ProcArrayInstallRestoredXmin is similar to ProcArrayInstallImportedXmin but operates with a direct PGPROC pointer rather than searching by virtual transaction ID. This function is used when restoring snapshots where the source transaction's PGPROC structure is already known.

The key difference from the imported version is that this function also copies status flags from the source PGPROC to ensure that VACUUM and other processes interpret the installed xmin correctly. This is crucial because certain status flags (PROC_XMIN_FLAGS) affect how the xmin value is processed during transaction horizon calculations.

The function performs essential safety checks:
1. **Database consistency**: Ensures the source transaction is in the same database
2. **Xmin validity**: Confirms the source PGPROC's xmin covers the requested xmin value
3. **Transaction state**: Verifies the source transaction is still valid

The function requires exclusive ProcArrayLock access (unlike the shared lock used by ProcArrayInstallImportedXmin) because it needs to update both xmin and statusFlags atomically. This prevents race conditions where status flags might be inconsistent with the xmin value.

## Parameters / Member Variables
- : The transaction ID to install as the new xmin value
- : Pointer to the PGPROC structure of the source transaction from which the snapshot was obtained

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsNormal (validates transaction ID format)
  - [TransactionIdPrecedesOrEquals](../T/TransactionIdPrecedesOrEquals.md) (checks xmin coverage)
  - UINT32_ACCESS_ONCE (atomic read of transaction ID)
  - LWLockAcquire/LWLockRelease (ProcArrayLock with LW_EXCLUSIVE mode)
  - PROC_XMIN_FLAGS (status flag mask for xmin-related flags)
- Called from (representative examples):
  - SetTransactionSnapshot (snapshot restoration operations)

## Notes and Other Information
- Returns true if the restoration was successful, false if the source transaction is no longer valid
- Requires ProcArrayLock in exclusive mode (stronger than ProcArrayInstallImportedXmin) to safely copy status flags
- Copies PROC_XMIN_FLAGS from the source PGPROC to maintain vacuum interpretation consistency
- Used when the source PGPROC is directly available, avoiding the need to search by virtual transaction ID
- The status flag copying ensures that vacuum processes correctly handle the restored xmin value
- Essential for maintaining MVCC consistency during snapshot restoration operations
- Updates both backend-local (MyProc) and global (ProcGlobal) status flag arrays for consistency
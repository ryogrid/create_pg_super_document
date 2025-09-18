# GetSerializableTransactionSnapshotInt

## Location
[src/backend/storage/lmgr/predicate.c:1754-1929](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L1754-L1929)

## Overview
Core internal implementation function that handles the actual setup of serializable transaction context, creating and initializing the SERIALIZABLEXACT structure with snapshot data and conflict tracking mechanisms.

## Definition
```c
static Snapshot GetSerializableTransactionSnapshotInt(Snapshot snapshot, VirtualTransactionId *sourcevxid, int sourcepid)
```

## Detailed Description
This is the main internal implementation function for both GetSerializableTransactionSnapshot and SetSerializableTransactionSnapshot. It performs the complex initialization required for serializable transactions including:

1. **Transaction Structure Creation**: Creates and initializes a SERIALIZABLEXACT structure to track this transaction's serializable state
2. **Snapshot Handling**: Either takes a new snapshot via GetSnapshotData() or validates an imported snapshot
3. **Read-Only Optimizations**: Implements several optimizations for read-only transactions, allowing them to "opt out" of predicate locking when safe
4. **Conflict Detection Setup**: For read-only transactions, registers all concurrent read-write transactions as possible unsafe conflicts
5. **Global State Management**: Maintains global serializable transaction state including global xmin tracking

The function includes sophisticated race condition handling, particularly for imported snapshots where it must verify the source transaction is still running. It also implements performance optimizations that allow read-only transactions to bypass the overhead of predicate locking when no dangerous patterns exist.

## Parameters / Member Variables
- `snapshot`: The snapshot structure to use (either to be filled by GetSnapshotData or already populated for import)
- `sourcevxid`: Virtual transaction ID of source transaction (NULL for new snapshots, valid for imported snapshots)
- `sourcepid`: Process ID of source transaction (used for error reporting in import scenarios)

## Dependencies
- Functions called/Symbols referenced:
  - [CreatePredXact](../C/CreatePredXact.md) (creates new SERIALIZABLEXACT structure)
  - [GetSnapshotData](GetSnapshotData.md) (obtains new snapshot data)
  - [ProcArrayInstallImportedXmin](../P/ProcArrayInstallImportedXmin.md) (validates imported snapshots)
  - [GetTopTransactionIdIfAny](GetTopTransactionIdIfAny.md) (gets current transaction ID if assigned)
  - [CreateLocalPredicateLockHash](../C/CreateLocalPredicateLockHash.md) (initializes local predicate lock tracking)
  - Various conflict tracking functions (SetPossibleUnsafeConflict, SxactIsCommitted, etc.)
  - [TransactionIdFollows](../T/TransactionIdFollows.md), TransactionIdEquals (transaction ID comparison utilities)
- Called from (representative examples):
  - [GetSerializableTransactionSnapshot](GetSerializableTransactionSnapshot.md) (for new snapshots)
  - [SetSerializableTransactionSnapshot](../S/SetSerializableTransactionSnapshot.md) (for imported snapshots)
  - GetSafeSnapshot (for deferrable read-only transactions)

## Notes and Other Information
- Static function - only used internally within predicate.c
- Cannot be called during parallel operations as all parts of a serializable transaction must use the same snapshot
- Includes sophisticated "opt-out" logic for read-only transactions when no write/write conflicts are possible
- Maintains complex global state including WritableSxactCount and SxactGlobalXmin
- Handles memory management by potentially calling SummarizeOldestCommittedSxact() to free old transaction structures
- Critical for PostgreSQL's Serializable Snapshot Isolation (SSI) implementation
- Uses extensive locking (SerializableXactHashLock) to prevent race conditions during initialization
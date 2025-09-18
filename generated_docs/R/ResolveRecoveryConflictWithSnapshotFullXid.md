# ResolveRecoveryConflictWithSnapshotFullXid

## Location
[src/backend/storage/ipc/standby.c:511-537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L511-L537)

## Overview
This function is a variant of ResolveRecoveryConflictWithSnapshot that operates with 64-bit FullTransactionId values, providing extended transaction ID range support for recovery conflict resolution.

## Definition
```c
void ResolveRecoveryConflictWithSnapshotFullXid(FullTransactionId snapshotConflictHorizon,
                                                 bool isCatalogRel,
                                                 RelFileLocator locator)
```

## Detailed Description
ResolveRecoveryConflictWithSnapshotFullXid extends PostgreSQL's snapshot conflict resolution capabilities to handle 64-bit FullTransactionId values, which provide a broader transaction ID range compared to the traditional 32-bit TransactionId. The function implements intelligent transaction ID range checking to determine if conflict resolution is necessary.

The core logic involves comparing the provided FullTransactionId against the current system's next transaction ID to determine if the conflict horizon is still relevant. If the difference between the current transaction ID and the conflict horizon is less than half of the maximum 32-bit transaction ID range (MaxTransactionId / 2), it indicates that the conflict horizon hasn't been affected by transaction ID wraparound and conflicts may still exist.

When conflicts are possible, the function truncates the FullTransactionId to a standard 32-bit TransactionId and delegates the actual conflict resolution to ResolveRecoveryConflictWithSnapshot. This design leverages the existing conflict resolution infrastructure while extending support for the expanded transaction ID space.

## Parameters / Member Variables
- `snapshotConflictHorizon`: FullTransactionId representing the 64-bit conflict boundary for snapshot visibility
- `isCatalogRel`: Boolean flag indicating whether the operation involves a catalog relation
- `locator`: RelFileLocator containing database OID and relation identification information

## Dependencies
- Functions called/Symbols referenced:
  - ReadNextFullTransactionId
  - U64FromFullTransactionId
  - XidFromFullTransactionId
  - [ResolveRecoveryConflictWithSnapshot](ResolveRecoveryConflictWithSnapshot.md)
  - MaxTransactionId
- Called from (representative examples):
  - [gistRedoPageReuse](../g/gistRedoPageReuse.md)
  - [btree_xlog_reuse_page](../b/btree_xlog_reuse_page.md)

## Notes and Other Information
- This function provides a bridge between the newer 64-bit FullTransactionId system and the existing 32-bit conflict resolution infrastructure
- The wraparound detection logic (diff < MaxTransactionId / 2) ensures that very old transaction IDs that have already wrapped around are safely ignored
- The function optimizes performance by avoiding unnecessary conflict resolution when the conflict horizon is too old to be relevant
- Primarily used in page reuse scenarios during WAL replay where extended transaction ID ranges are logged
- The truncation from FullTransactionId to TransactionId is safe due to the wraparound check that ensures the value is still within the relevant range
- This design maintains backward compatibility with existing conflict resolution mechanisms while supporting PostgreSQL's extended transaction ID capabilities
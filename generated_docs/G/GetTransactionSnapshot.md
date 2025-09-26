# GetTransactionSnapshot

## Location
[src/backend/utils/time/snapmgr.c:216-290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L216-L290)

## Overview
Obtains the appropriate snapshot for a new query in a transaction, handling various isolation levels and transaction states.

## Definition
```c
Snapshot GetTransactionSnapshot(void)
```

## Detailed Description
GetTransactionSnapshot is the primary function for obtaining a snapshot that represents the transactions view of the database state. It handles different scenarios based on the transactions isolation level, whether its the first snapshot in the transaction, and special cases like logical decoding and parallel operations. The function ensures proper snapshot lifecycle management and maintains consistency with PostgreSQLs MVCC (Multi-Version Concurrency Control) system.

Key behaviors:
- For logical decoding, returns the historic snapshot
- For the first snapshot in a transaction, performs initialization and validation
- Handles transaction-snapshot isolation mode by creating persistent snapshots
- Supports both serializable and other isolation levels
- Integrates with the catalog snapshot system for consistency

## Parameters / Member Variables
- Returns: A Snapshot representing the current transactions view of the database

## Dependencies
- Functions called/Symbols referenced:
  - HistoricSnapshotActive
  - InvalidateCatalogSnapshot
  - pairingheap_is_empty
  - IsInParallelMode
  - IsolationUsesXactSnapshot
  - IsolationIsSerializable
  - GetSerializableTransactionSnapshot
  - GetSnapshotData
  - CopySnapshot
  - pairingheap_add
- Called from (representative examples):
  - _brin_begin_parallel
  - heapam_index_build_range_scan
  - exec_simple_query
  - PortalStart
  - InitPostgres
  - GetLatestSnapshot

## Notes and Other Information
- The returned snapshot may point to static storage that gets modified by future calls
- Callers should use RegisterSnapshot or PushActiveSnapshot if the snapshot needs to persist
- First call in a transaction triggers special initialization logic
- Prevents taking snapshots during parallel operations for safety
- In transaction-snapshot mode, the first snapshot persists until transaction end
- Maintains coordination with catalog snapshots to prevent inconsistencies
- Critical for PostgreSQLs MVCC implementation and transaction isolation
- The function is performance-sensitive as its called frequently during query execution
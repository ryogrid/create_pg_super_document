# HistoricSnapshotGetTupleCids

## Location
[src/backend/utils/time/snapmgr.c:1678-1691](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L1678-L1691)

## Overview
HistoricSnapshotGetTupleCids returns the hash table containing tuple command ID (cmin, cmax) data that was set up during historical snapshot initialization for logical decoding operations.

## Definition
```c
HTAB *HistoricSnapshotGetTupleCids(void)
```

## Detailed Description
This function provides access to the tuplecid_data hash table that was configured when SetupHistoricSnapshot was called. The hash table contains mappings of tuple command IDs (cmin, cmax) that are essential for determining tuple visibility during logical decoding when using historical snapshots.

The function includes an assertion to ensure that a historical snapshot is currently active before returning the tuple CID data. This prevents accidental access to potentially invalid or NULL data when historical snapshot mode is not enabled.

The returned hash table is used by visibility checking functions to determine whether specific tuples were visible at the historical point in time being decoded, which is crucial for maintaining consistency in logical replication.

## Parameters / Member Variables
- None (void function with no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - HistoricSnapshotActive (assertion check)
  - Assert (assertion macro)
- Called from (representative examples):
  - HeapTupleSatisfiesHistoricMVCC (in heap visibility checks, multiple locations)

## Notes and Other Information
- The function must only be called when HistoricSnapshotActive() returns true
- Returns the global tuplecid_data hash table that was set up by SetupHistoricSnapshot
- Essential for tuple visibility determination during logical decoding operations
- The returned HTAB pointer should not be modified or freed by the caller
- Located in src/backend/utils/time/snapmgr.c at lines 1678-1691
- Used primarily by heap tuple visibility checking functions during historical MVCC operations
- The assertion ensures program correctness by preventing calls when no historical snapshot is active
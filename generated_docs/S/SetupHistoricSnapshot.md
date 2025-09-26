# SetupHistoricSnapshot

## Location
src/backend/utils/time/snapmgr.c: 1649 - 1664

## Overview
SetupHistoricSnapshot establishes a snapshot that replaces normal catalog snapshots, allowing catalog access to behave as it did at a specific point in the past, primarily needed for logical decoding operations.

## Definition
```c
void SetupHistoricSnapshot(Snapshot historic_snapshot, HTAB *tuplecids)
```

## Detailed Description
This function is a core component of PostgreSQL's logical decoding infrastructure. It configures the system to use a historical snapshot for catalog lookups instead of the current snapshot. This functionality is essential for logical decoding because it allows the decoder to see the database schema as it existed when the changes being decoded were originally made.

The function performs two main setup operations:
1. Sets the global HistoricSnapshot variable to the provided historical snapshot
2. Configures the tuplecid_data hash table for (cmin, cmax) lookups

This setup ensures that when catalog tables are accessed during logical decoding, they reflect the state of the database at the time the decoded changes were originally committed, rather than the current state.

## Parameters / Member Variables
- `historic_snapshot`: A Snapshot structure representing the historical point in time to which catalog access should be restricted. Must not be NULL.
- `tuplecids`: A hash table (HTAB*) used for looking up command IDs (cmin, cmax) associated with tuples during the historical snapshot period.

## Dependencies
- Functions called/Symbols referenced:
  - HTAB (hash table data type)
  - Assert (assertion macro)
- Called from (representative examples):
  - ReorderBufferQueueMessage (in logical reorderbuffer)
  - ReorderBufferProcessTXN (in logical reorderbuffer, multiple locations)

## Notes and Other Information
- This function is specifically designed for logical decoding functionality
- The function modifies global variables HistoricSnapshot and tuplecid_data
- Must be paired with TeardownHistoricSnapshot to clean up the historical snapshot setup
- The historic_snapshot parameter is validated with an Assert to ensure it's not NULL
- Located in src/backend/utils/time/snapmgr.c at lines 1649-1664
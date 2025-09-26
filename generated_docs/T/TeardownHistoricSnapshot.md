# TeardownHistoricSnapshot

## Location
[src/backend/utils/time/snapmgr.c:1665-1671](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L1665-L1671)

## Overview
TeardownHistoricSnapshot restores catalog snapshots to normal behavior by clearing the historical snapshot setup, effectively ending the timetravel snapshot mode used for logical decoding.

## Definition
```c
void TeardownHistoricSnapshot(bool is_error)
```

## Detailed Description
This function serves as the cleanup counterpart to SetupHistoricSnapshot. It resets the global state variables that were configured to enable historical catalog access during logical decoding operations. By setting both HistoricSnapshot and tuplecid_data to NULL, it ensures that subsequent catalog lookups will use the current snapshot rather than the historical one.

The function is designed to be called when logical decoding operations are complete or when an error occurs during processing that requires cleanup. It provides a clean way to exit from the historical snapshot mode and return to normal database operations.

## Parameters / Member Variables
- `is_error`: A boolean flag indicating whether the teardown is happening due to an error condition. While the parameter is accepted, it's not currently used in the function implementation but may be reserved for future error handling logic.

## Dependencies
- Functions called/Symbols referenced:
  - None (only sets global variables to NULL)
- Called from (representative examples):
  - ReorderBufferQueueMessage (in logical reorderbuffer, multiple locations)
  - ReorderBufferProcessTXN (in logical reorderbuffer, multiple locations)
  - CHANGES_THRESHOLD (in logical reorderbuffer context)

## Notes and Other Information
- This function must be called after SetupHistoricSnapshot to properly clean up the historical snapshot state
- The function is part of the logical decoding infrastructure in PostgreSQL
- Despite accepting an is_error parameter, the current implementation treats all teardown scenarios identically
- The function resets global variables HistoricSnapshot and tuplecid_data to NULL
- Located in src/backend/utils/time/snapmgr.c at lines 1665-1671
- Essential for preventing memory leaks and ensuring proper state management in logical decoding operations
# RunFromStore

## Location
src/backend/tcop/pquery.c: 1059 - 1124

## Overview
Fetches tuples from a portal's tuple store and sends them to a destination receiver, providing a mechanism to retrieve previously stored query results.

## Definition


## Detailed Description
RunFromStore is a specialized function that retrieves tuples from a portal's tuple store and delivers them to a destination receiver. Unlike ExecutorRun, this function operates without a queryDesc or estate, making it suitable for fetching previously stored results. The function creates a temporary tuple slot, iterates through the stored tuples in the specified direction, and sends each tuple to the destination receiver. It handles scan direction logic, respects tuple count limits, and properly manages memory contexts. The function returns the number of tuples processed and ensures proper cleanup of resources.

## Parameters / Member Variables
- : The Portal structure containing the tuple store to read from
- : ScanDirection indicating whether to scan forward, backward, or no movement
- : Maximum number of tuples to fetch (0 means no limit)
- : DestReceiver that will process the retrieved tuples

## Dependencies
- Functions called/Symbols referenced:
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - ScanDirectionIsNoMovement
  - ScanDirectionIsForward
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md)
  - ExecClearTuple
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md)
- Called from (representative examples):
  - [PortalRunSelect](../P/PortalRunSelect.md)

## Notes and Other Information
- This function is static and only used within pquery.c
- Returns the actual number of tuples processed as a uint64 value
- Operates in the caller's memory context since there's no estate available
- Handles memory context switching when accessing the portal's hold store
- Supports both forward and backward scanning through the tuple store
- Respects destination receiver feedback - stops if receiver indicates it can't accept more tuples
- Properly cleans up the temporary tuple slot after use
- Used for retrieving results from portals that have been filled by FillPortalStore
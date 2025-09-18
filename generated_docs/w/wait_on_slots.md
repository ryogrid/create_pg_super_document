# wait_on_slots

## Location
src/fe_utils/parallel_slot.c: 196 - 286

## Overview
wait_on_slots is a static function that waits for any parallel slot connection to receive query results, processes those results, and updates slot status accordingly.

## Definition


## Detailed Description
This function implements the core waiting and result processing logic for parallel slot operations. It uses select() to wait for input on any of the active connection sockets, then processes available results from those connections. The function reconstructs the file descriptor set for each call, waits for activity using select_loop(), and then processes all available results from connections that have data ready. When a connection completes its query and returns NULL from PQgetResult(), the slot is marked as idle and available for reuse.

## Parameters / Member Variables
- : Pointer to the ParallelSlotArray structure containing the slots to monitor

## Dependencies
- Functions called/Symbols referenced:
  - [PQsocket](../P/PQsocket.md) (gets socket file descriptor from connection)
  - [SetCancelConn](../S/SetCancelConn.md) (sets up cancellation handling)
  - [select_loop](../s/select_loop.md) (waits for socket activity)
  - [ResetCancelConn](../R/ResetCancelConn.md) (cleans up cancellation handling)
  - [PQconsumeInput](../P/PQconsumeInput.md) (consumes available input from connection)
  - [PQisBusy](../P/PQisBusy.md) (checks if connection is busy processing)
  - [PQgetResult](../P/PQgetResult.md) (retrieves query result)
  - [processQueryResult](../p/processQueryResult.md) (handles query result)
  - [ParallelSlotClearHandler](../P/ParallelSlotClearHandler.md) (cleans up slot handler)
- Called from (representative examples):
  - [ParallelSlotsGetIdle](../P/ParallelSlotsGetIdle.md)

## Notes and Other Information
- Returns true on success, false on cancellation, error, or if no slots are connected
- Uses file descriptor sets and select() for efficient waiting on multiple connections
- Processes all available results from each connection that has data ready
- Marks slots as idle when their queries complete (PQgetResult returns NULL)
- Handles connection socket errors gracefully by skipping invalid sockets
- Uses the first valid connection found for cancellation handling
- The function is static, limiting its scope to the parallel_slot.c compilation unit
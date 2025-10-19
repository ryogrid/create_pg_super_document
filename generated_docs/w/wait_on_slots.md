# wait_on_slots

## Location
[src/fe_utils/parallel_slot.c:196-286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/parallel_slot.c#L196-L286)

## Overview
wait_on_slots is a static function that waits for any parallel slot connection to receive query results, processes those results, and updates slot status accordingly.

## Definition

```c
struct the fd_set for each call to select_loop */
	FD_ZERO(&slotset);
```
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

## Simplified Source

```c
static bool wait_on_slots(ParallelSlotArray *sa) {
    int i;
    fd_set slotset;
    int maxFd = 0;
    PGconn *cancelconn = NULL;

    // Build file descriptor set for select()
    FD_ZERO(&slotset);
    for (i = 0; i < sa->numslots; i++) {
        int sock = PQsocket(sa->slots[i].connection);

        if (sock < 0) continue;  // Skip invalid sockets

        if (cancelconn == NULL) {
            cancelconn = sa->slots[i].connection;  // First valid connection
        }

        FD_SET(sock, &slotset);
        if (sock > maxFd) maxFd = sock;
    }

    // No valid connections
    if (cancelconn == NULL) return false;

    // Wait for activity on any socket
    SetCancelConn(cancelconn);
    i = select_loop(maxFd, &slotset);
    ResetCancelConn();

    if (i < 0) return false;  // select() failed

    // Process results from connections with data
    for (i = 0; i < sa->numslots; i++) {
        int sock = PQsocket(sa->slots[i].connection);

        // Consume input if socket has data
        if (sock >= 0 && FD_ISSET(sock, &slotset)) {
            PQconsumeInput(sa->slots[i].connection);
        }

        // Process all available results
        while (!PQisBusy(sa->slots[i].connection)) {
            PGresult *result = PQgetResult(sa->slots[i].connection);

            if (result != NULL) {
                // Process the result
                if (!processQueryResult(&sa->slots[i], result)) {
                    return false;
                }
            } else {
                // Query completed - mark slot as idle
                sa->slots[i].inUse = false;
                ParallelSlotClearHandler(&sa->slots[i]);
                break;
            }
        }
    }

    return true;
}
```
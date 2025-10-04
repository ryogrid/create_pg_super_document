# getCopyDataMessage

## Location
[src/interfaces/libpq/fe-protocol3.c:1642-1750](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L1642-L1750)

## Overview
Fetches the next CopyData message from the network stream while processing asynchronous messages that may arrive during COPY operations.

## Definition
```c
static int getCopyDataMessage(PGconn *conn)
```

## Detailed Description
The getCopyDataMessage function implements a message processing loop specifically designed for COPY operations. It reads incoming messages from the network stream and handles both COPY-related messages (CopyData, CopyDone) and asynchronous messages (notifications, notices, parameter status updates) that can arrive at any time during a COPY operation.

The function operates in a loop, examining each incoming message and taking appropriate action based on the message type. For COPY protocol messages, it either returns the message length (for CopyData) or signals end-of-copy conditions. For asynchronous messages, it processes them and continues looking for the next message. This design ensures that asynchronous server messages don't get lost during long-running COPY operations.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object containing the network stream and connection state

## Dependencies
- Functions called/Symbols referenced:
  - [pqGetc](../p/pqGetc.md)
  - [pqGetInt](../p/pqGetInt.md)
  - [handleSyncLoss](../h/handleSyncLoss.md)
  - [pqCheckInBufferSpace](../p/pqCheckInBufferSpace.md)
  - [getNotify](getNotify.md)
  - [pqGetErrorNotice3](../p/pqGetErrorNotice3.md)
  - [getParameterStatus](getParameterStatus.md)
  - [pqTraceOutputMessage](../p/pqTraceOutputMessage.md)
  - PqMsg_NotificationResponse, PqMsg_NoticeResponse, PqMsg_ParameterStatus, PqMsg_CopyData, PqMsg_CopyDone
  - PGASYNC_COPY_BOTH, PGASYNC_COPY_IN, PGASYNC_BUSY
- Called from (representative examples):
  - [pqGetCopyData3](../p/pqGetCopyData3.md)
  - [pqGetlineAsync3](../p/pqGetlineAsync3.md)

## Notes and Other Information
- Returns: message length (> 0) for CopyData, 0 if no complete message available, -1 if end of copy, -2 if error
- Handles buffer management to ensure complete messages can be read
- Properly manages connection async status transitions during COPY_BOTH operations
- Processes asynchronous messages transparently during COPY operations
- Includes protocol debugging support via pqTraceOutputMessage
- Critical for maintaining protocol integrity during long-running COPY operations
- Implements robust error handling including sync loss recovery

## Simplified Source

```c
static int getCopyDataMessage(PGconn *conn) {
    char id;
    int msgLength;
    int avail;

    for (;;) {
        // Read message header (type and length)
        conn->inCursor = conn->inStart;
        if (pqGetc(&id, conn)) return 0;
        if (pqGetInt(&msgLength, 4, conn)) return 0;

        // Validate message length
        if (msgLength < 4) {
            handleSyncLoss(conn, id, msgLength);
            return -2;
        }

        // Check if complete message is available
        avail = conn->inEnd - conn->inCursor;
        if (avail < msgLength - 4) {
            // Enlarge buffer if needed for complete message
            if (pqCheckInBufferSpace(conn->inCursor + (size_t) msgLength - 4, conn)) {
                handleSyncLoss(conn, id, msgLength);
                return -2;
            }
            return 0;  // Wait for more data
        }

        // Process message based on type
        switch (id) {
            case PqMsg_NotificationResponse:
                if (getNotify(conn)) return 0;
                break;
            case PqMsg_NoticeResponse:
                if (pqGetErrorNotice3(conn, false)) return 0;
                break;
            case PqMsg_ParameterStatus:
                if (getParameterStatus(conn)) return 0;
                break;
            case PqMsg_CopyData:
                return msgLength;  // Return length of CopyData message
            case PqMsg_CopyDone:
                // Handle COPY mode transitions
                if (conn->asyncStatus == PGASYNC_COPY_BOTH)
                    conn->asyncStatus = PGASYNC_COPY_IN;
                else
                    conn->asyncStatus = PGASYNC_BUSY;
                return -1;  // End of copy
            default:
                // Any other message terminates copy mode
                conn->asyncStatus = PGASYNC_BUSY;
                return -1;
        }

        // Trace message if debugging enabled
        if (conn->Pfdebug)
            pqTraceOutputMessage(conn, conn->inBuffer + conn->inStart, false);

        // Move to next message
        conn->inStart = conn->inCursor;
    }
}
```
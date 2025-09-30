# ReceiveCopyBegin

## Location
[src/backend/commands/copyfromparse.c:170-189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfromparse.c#L170-L189)

## Overview
Initiates the COPY FROM protocol by sending a CopyInResponse message to the frontend, indicating that the server is ready to receive copy data from the client.

## Definition

```c
void
ReceiveCopyBegin(CopyFromState cstate)
```
## Detailed Description
ReceiveCopyBegin sets up the COPY FROM operation by sending a CopyInResponse message to the frontend. This function prepares the communication channel for receiving copy data by specifying the format (text or binary) and the number of columns expected. It also initializes the frontend message buffer and sets the copy source to COPY_FRONTEND. The function ensures that the frontend receives this information by flushing the output buffer immediately.

## Parameters / Member Variables
- : CopyFromState structure containing the current state and configuration of the COPY FROM operation, including format options and attribute information

## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md) (to get number of attributes)
  - [pq_beginmessage](../p/pq_beginmessage.md) (start building protocol message)
  - [pq_sendbyte](../p/pq_sendbyte.md) (send format byte)
  - [pq_sendint16](../p/pq_sendint16.md) (send column count and per-column formats)
  - [pq_endmessage](../p/pq_endmessage.md) (complete the message)
  - [makeStringInfo](../m/makeStringInfo.md) (create frontend message buffer)
  - pq_flush (ensure message is sent immediately)
- Called from (representative examples):
  - [BeginCopyFrom](../B/BeginCopyFrom.md) (src/backend/commands/copyfrom.c:1708)

## Notes and Other Information
- The function must flush immediately to ensure the frontend knows it can start sending data
- Sets up both the overall format and per-column format specifications in the protocol message
- Initializes the frontend message buffer that will be used to receive incoming data
- The format field indicates whether the copy operation uses text (0) or binary (1) format

## Simplified Source
```c
void ReceiveCopyBegin(CopyFromState cstate) {
    // Prepare CopyInResponse message for frontend
    StringInfoData buf;
    int natts = list_length(cstate->attnumlist);
    int16 format = (cstate->opts.binary ? 1 : 0);  // 0=text, 1=binary

    // Build protocol message
    pq_beginmessage(&buf, PqMsg_CopyInResponse);
    pq_sendbyte(&buf, format);        // overall format
    pq_sendint16(&buf, natts);        // number of columns

    // Send format for each column
    for (int i = 0; i < natts; i++) {
        pq_sendint16(&buf, format);
    }

    pq_endmessage(&buf);

    // Initialize copy state for frontend communication
    cstate->copy_src = COPY_FRONTEND;
    cstate->fe_msgbuf = makeStringInfo();

    // Must flush to signal frontend can start sending data
    pq_flush();
}
```
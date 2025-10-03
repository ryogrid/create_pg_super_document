# SendCopyBegin

## Location
[src/backend/commands/copyto.c:133-149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyto.c#L133-L149)

## Overview
SendCopyBegin is a static function that initiates the COPY TO protocol by sending a CopyOutResponse message to the frontend client, establishing the format and column information for the data transfer.

## Definition

```c
static void
SendCopyBegin(CopyToState cstate)
```
## Detailed Description
This function is responsible for starting the frontend copy-out operation by sending the initial protocol message that informs the client about the format of the data that will be sent. It constructs a CopyOutResponse message containing the overall format (binary or text), the number of columns, and the format for each individual column. The function sets up the communication protocol between PostgreSQL backend and the client for COPY TO operations, ensuring both sides understand the data format before actual data transmission begins.

## Parameters / Member Variables
- `cstate`: Pointer to CopyToState structure containing the state information for the copy operation, including format options, attribute list, and destination settings
## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md) (to get number of attributes)
  - [pq_beginmessage](../p/pq_beginmessage.md) (to start building the protocol message)
  - [pq_sendbyte](../p/pq_sendbyte.md) (to send the overall format byte)
  - [pq_sendint16](../p/pq_sendint16.md) (to send column count and per-column formats)
  - [pq_endmessage](../p/pq_endmessage.md) (to finalize and send the message)
  - PqMsg_CopyOutResponse (message type constant)
  - COPY_FRONTEND (destination type constant)
- Called from (representative examples):
  - DR_copy (in copyto.c:118)
  - [DoCopyTo](../D/DoCopyTo.md) (in copyto.c:757)

## Notes and Other Information
- The function sets the copy destination to COPY_FRONTEND after sending the begin message
- Format value of 0 indicates text format, 1 indicates binary format
- The same format is applied to all columns in the current implementation
- This is part of the PostgreSQL frontend/backend protocol for COPY operations
- The function is static, meaning it's only accessible within the copyto.c file

## Simplified Source

```c
static void
SendCopyBegin(CopyToState cstate)
{
    StringInfoData buf;
    int natts = list_length(cstate->attnumlist);
    int16 format = (cstate->opts.binary ? 1 : 0);

    // Send CopyOutResponse message to client
    pq_beginmessage(&buf, PqMsg_CopyOutResponse);
    pq_sendbyte(&buf, format);        // Overall format (0=text, 1=binary)
    pq_sendint16(&buf, natts);        // Number of columns

    // Send format for each column (same as overall format)
    for (int i = 0; i < natts; i++)
        pq_sendint16(&buf, format);

    pq_endmessage(&buf);
    cstate->copy_dest = COPY_FRONTEND;
}
```
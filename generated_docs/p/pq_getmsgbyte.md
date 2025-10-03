# pq_getmsgbyte

## Location
[src/backend/libpq/pqformat.c:399-414](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqformat.c#L399-L414)

## Overview
Extracts a single byte from a message buffer and advances the cursor position.

## Definition

```c
int
pq_getmsgbyte(StringInfo msg)
```
## Detailed Description
The  function is a fundamental message parsing utility in PostgreSQL's libpq communication protocol. It reads one raw byte from a message buffer (StringInfo) at the current cursor position and automatically advances the cursor for subsequent reads. The function includes bounds checking to ensure data integrity during message parsing operations. If the cursor has reached or exceeded the message length, it raises a protocol violation error rather than allowing buffer overruns.

## Parameters / Member Variables
- `msg`: A StringInfo structure containing the message buffer data, length, and current cursor position
## Dependencies
- Functions called/Symbols referenced:
  - ereport (error reporting mechanism)
  - [errcode](../e/errcode.md) (error code specification)
  - [errmsg](../e/errmsg.md) (error message formatting)
- Called from (representative examples):
  - [HandleParallelMessage](../H/HandleParallelMessage.md)
  - [logicalrep_read_commit](../l/logicalrep_read_commit.md)
  - [LogicalRepApplyLoop](../L/LogicalRepApplyLoop.md)
  - [boolrecv](../b/boolrecv.md)
  - [charrecv](../c/charrecv.md)
  - [macaddr_recv](../m/macaddr_recv.md)
  - [network_recv](../n/network_recv.md)

## Notes and Other Information
- Returns the byte value as an int (cast from unsigned char)
- Automatically increments the cursor position after reading
- Part of PostgreSQL's binary message format parsing infrastructure
- Used extensively in logical replication, parallel processing, and data type deserialization
- Throws ERRCODE_PROTOCOL_VIOLATION if attempting to read beyond message boundaries
- The function is defined in src/backend/libpq/pqformat.c:399-414

## Simplified Source

```c
// Simplified version of pq_getmsgbyte
int pq_getmsgbyte(StringInfo msg) {
    // Check if we have data left to read
    if (msg->cursor >= msg->len) {
        // Report protocol violation error if no data left
        ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                 errmsg("no data left in message")));
    }

    // Read byte at current position and advance cursor
    return (unsigned char) msg->data[msg->cursor++];
}
```

Key simplifications made:
- Added clear comments explaining each step
- Maintained the essential bounds checking logic
- Preserved the cursor increment behavior
- Kept the critical error handling for protocol violations
- Formatted for improved readability
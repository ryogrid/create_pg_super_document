# logicalrep_write_origin

## Location
[src/backend/replication/logical/proto.c:385-400](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L385-L400)

## Overview
This function writes an ORIGIN message to the logical replication output stream, used to track replication origin information during logical replication.

## Definition

```c
void
logicalrep_write_origin(StringInfo out, const char *origin,
						XLogRecPtr origin_lsn)
```
## Detailed Description
The  function serializes replication origin information into the logical replication stream. It creates a message with the  type, followed by the origin LSN position and the origin name string. This function is essential for tracking the provenance of replicated changes, allowing subscribers to understand where specific changes originated from in a multi-master or cascading replication setup.

The message format includes a message type byte, the 64-bit LSN where the origin was recorded, and a null-terminated string containing the origin name. This information enables proper handling of changes that may have originated from different replication sources.

## Parameters / Member Variables
- : StringInfo buffer where the serialized ORIGIN message will be written
- : Null-terminated string containing the name of the replication origin
- : XLogRecPtr indicating the LSN position associated with this origin

## Dependencies
- Functions called/Symbols referenced:
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - LOGICAL_REP_MSG_ORIGIN
  - [pq_sendint64](../p/pq_sendint64.md)
  - [pq_sendstring](../p/pq_sendstring.md)
- Called from (representative examples):
  - [send_repl_origin](../s/send_repl_origin.md)

## Notes and Other Information
- Part of PostgreSQL's replication origin tracking system
- Essential for preventing replication loops in multi-master setups
- The origin parameter should be a valid null-terminated string
- Located in src/backend/replication/logical/proto.c:385-400
- Uses PostgreSQL's binary protocol functions for message serialization
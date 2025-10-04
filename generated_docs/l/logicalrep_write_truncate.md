# logicalrep_write_truncate

## Location
[src/backend/replication/logical/proto.c:586-617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L586-L617)

## Overview
Serializes and writes a TRUNCATE message to the logical replication output stream for transmission to subscribers.

## Definition

```c
void
logicalrep_write_truncate(StringInfo out,
						  TransactionId xid,
						  int nrelids,
						  Oid relids[],
						  bool cascade, bool restart_seqs)
```
## Detailed Description
This function encodes a TRUNCATE operation into the logical replication protocol format. It writes the message type identifier followed by the transaction ID (if valid), the number of relations being truncated, truncation flags (cascade and restart sequences options), and the OIDs of all relations to be truncated. The function is part of PostgreSQL's logical replication protocol implementation that allows streaming of database changes to subscribers.

## Parameters / Member Variables
- `out`: StringInfo buffer where the serialized truncate message will be written
- `xid`: Transaction ID associated with the truncate operation (only sent if valid)
- `nrelids`: Number of relations being truncated in this operation
- `relids[]`: Array of relation OIDs that are being truncated
- `cascade`: Boolean flag indicating whether the truncate should cascade to referencing tables
- `restart_seqs`: Boolean flag indicating whether sequences should be restarted

## Dependencies
- Functions called/Symbols referenced:
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - [pq_sendint32](../p/pq_sendint32.md)
  - [pq_sendint8](../p/pq_sendint8.md)
  - LOGICAL_REP_MSG_TRUNCATE
  - TRUNCATE_CASCADE
  - TRUNCATE_RESTART_SEQS
- Called from (representative examples):
  - [pgoutput_truncate](../p/pgoutput_truncate.md)

## Notes and Other Information
- Part of the logical replication protocol infrastructure
- Uses PostgreSQL's pq_send* family of functions for binary serialization
- Flags are encoded as a single byte with bitwise OR operations
- Transaction ID is conditionally sent only when valid (for streaming transactions)
- The message format follows the logical replication protocol specification

## Simplified Source

```c
void logicalrep_write_truncate(StringInfo out, TransactionId xid, int nrelids,
                              Oid relids[], bool cascade, bool restart_seqs) {
    int i;
    uint8 flags = 0;

    // Write message type
    pq_sendbyte(out, LOGICAL_REP_MSG_TRUNCATE);

    // Include transaction ID if we're streaming
    if (TransactionIdIsValid(xid))
        pq_sendint32(out, xid);

    // Write number of relations
    pq_sendint32(out, nrelids);

    // Encode truncate options as flags
    if (cascade)
        flags |= TRUNCATE_CASCADE;
    if (restart_seqs)
        flags |= TRUNCATE_RESTART_SEQS;
    pq_sendint8(out, flags);

    // Write all relation OIDs
    for (i = 0; i < nrelids; i++)
        pq_sendint32(out, relids[i]);
}
```
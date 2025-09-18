# logicalrep_write_truncate

## Location
src/backend/replication/logical/proto.c: 586 - 617

## Overview
Serializes and writes a TRUNCATE message to the logical replication output stream for transmission to subscribers.

## Definition


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
  - pq_sendbyte
  - pq_sendint32
  - pq_sendint8
  - LOGICAL_REP_MSG_TRUNCATE
  - TRUNCATE_CASCADE
  - TRUNCATE_RESTART_SEQS
- Called from (representative examples):
  - pgoutput_truncate

## Notes and Other Information
- Part of the logical replication protocol infrastructure
- Uses PostgreSQL's pq_send* family of functions for binary serialization
- Flags are encoded as a single byte with bitwise OR operations
- Transaction ID is conditionally sent only when valid (for streaming transactions)
- The message format follows the logical replication protocol specification
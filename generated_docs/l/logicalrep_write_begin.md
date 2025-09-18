# logicalrep_write_begin

## Location
src/backend/replication/logical/proto.c: 60 - 73

## Overview
Writes a BEGIN message to the logical replication output stream to indicate the start of a transaction.

## Definition


## Detailed Description
This function serializes a BEGIN message into the logical replication protocol stream. It marks the beginning of a transaction in the replication stream and includes essential transaction metadata such as the final LSN, commit timestamp, and transaction ID. The function is part of PostgreSQL's logical replication protocol implementation and is used to communicate transaction boundaries to logical replication subscribers.

The message format follows the logical replication protocol specification, starting with a message type identifier followed by fixed-length fields containing transaction information.

## Parameters / Member Variables
- : StringInfo buffer where the serialized BEGIN message will be written
- : ReorderBufferTXN structure containing transaction information to be serialized

## Dependencies
- Functions called/Symbols referenced:
  - pq_sendbyte (sends a single byte to the output buffer)
  - pq_sendint64 (sends a 64-bit integer to the output buffer)
  - pq_sendint32 (sends a 32-bit integer to the output buffer)
  - LOGICAL_REP_MSG_BEGIN (message type constant for BEGIN messages)
  - ReorderBufferTXN (transaction structure type)
- Called from (representative examples):
  - pgoutput_send_begin (in the pgoutput plugin for sending BEGIN messages to subscribers)

## Notes and Other Information
- This function is part of the logical replication protocol implementation
- The BEGIN message includes transaction final LSN, commit time, and transaction ID as fixed fields
- Used by logical replication output plugins to communicate transaction start to subscribers
- The message format is standardized and must be compatible with logical replication subscribers
- Located in src/backend/replication/logical/proto.c as part of the protocol encoding functions
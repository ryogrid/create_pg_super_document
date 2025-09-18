# logicalrep_write_prepare_common

## Location
src/backend/replication/logical/proto.c: 166 - 197

## Overview
A static helper function that provides core functionality for writing PREPARE messages in logical replication, shared between regular and streaming prepare operations.

## Definition
```c
static void logicalrep_write_prepare_common(StringInfo out, LogicalRepMsgType type, ReorderBufferTXN *txn, XLogRecPtr prepare_lsn)
```

## Detailed Description
This internal function encapsulates the common logic for serializing PREPARE messages in logical replication. It handles both regular PREPARE and STREAM PREPARE messages by accepting the message type as a parameter. The function validates that the transaction is properly prepared with assertions, then writes the message type, flags, LSN positions, timing information, transaction ID, and global transaction identifier to the output stream. This design promotes code reuse between different prepare message variants.

## Parameters / Member Variables
- `out`: StringInfo buffer where the serialized message will be written
- `type`: LogicalRepMsgType indicating the specific prepare message type to write
- `txn`: ReorderBufferTXN structure containing transaction information to be serialized
- `prepare_lsn`: XLogRecPtr specifying the LSN where the prepare occurred

## Dependencies
- Functions called/Symbols referenced:
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - [pq_sendint64](../p/pq_sendint64.md)
  - [pq_sendint32](../p/pq_sendint32.md)
  - [pq_sendstring](../p/pq_sendstring.md)
  - rbtxn_prepared
  - Assert
  - TransactionIdIsValid
  - LogicalRepMsgType
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md)
- Called from (representative examples):
  - [logicalrep_write_prepare](logicalrep_write_prepare.md)
  - [logicalrep_write_stream_prepare](logicalrep_write_stream_prepare.md)

## Notes and Other Information
- Static function internal to proto.c, not exposed in header files
- Includes assertions to validate transaction state: valid GID, prepared state, and valid XID
- Currently sends flags field as 0, potentially for future extensibility
- Designed to reduce code duplication between prepare and stream prepare message writing
- Located in src/backend/replication/logical/proto.c:166-197
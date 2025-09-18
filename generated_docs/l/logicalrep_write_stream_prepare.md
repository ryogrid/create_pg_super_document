# logicalrep_write_stream_prepare

## Location
src/backend/replication/logical/proto.c: 364 - 375

## Overview
This function writes a STREAM PREPARE message to the logical replication output stream, used in PostgreSQL's two-phase commit protocol for streaming transactions.

## Definition


## Detailed Description
The  function is a specialized wrapper that creates a logical replication message for preparing a streamed transaction. It delegates to the common preparation logic by calling  with the  message type. This function is part of PostgreSQL's logical replication protocol implementation, specifically handling the prepare phase of two-phase commit transactions that are being streamed.

The function ensures that transaction preparation information is properly serialized into the replication stream, including the transaction's prepare LSN, end LSN, prepare timestamp, transaction ID, and global identifier (GID).

## Parameters / Member Variables
- : StringInfo buffer where the serialized STREAM PREPARE message will be written
- : ReorderBufferTXN structure containing the transaction being prepared, must have a valid GID for two-phase commit
- : XLogRecPtr indicating the LSN where the prepare record was written in the WAL

## Dependencies
- Functions called/Symbols referenced:
  - logicalrep_write_prepare_common
  - LOGICAL_REP_MSG_STREAM_PREPARE
  - ReorderBufferTXN (type)
- Called from (representative examples):
  - pgoutput_stream_prepare_txn

## Notes and Other Information
- This function is specifically designed for streaming transactions in two-phase commit scenarios
- The transaction must have a valid GID and be in prepared state
- Part of the logical replication protocol message formatting system
- Located in src/backend/replication/logical/proto.c:364-375
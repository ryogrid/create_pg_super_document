# logicalrep_write_insert

## Location
src/backend/replication/logical/proto.c: 414 - 435

## Overview
This function writes an INSERT message to the logical replication output stream, serializing information about a newly inserted tuple for replication.

## Definition


## Detailed Description
The  function serializes an INSERT operation into the logical replication stream format. It creates a message with the  type, followed by optional transaction ID (for streaming transactions), the relation OID, and the actual tuple data. The function marks the tuple data with 'N' (indicating "new tuple") before delegating the actual tuple serialization to .

This function is a core component of PostgreSQL's logical replication system, responsible for transmitting INSERT operations to logical replication subscribers. It handles both streaming and non-streaming transaction contexts and supports binary format transmission when requested.

## Parameters / Member Variables
- : StringInfo buffer where the serialized INSERT message will be written
- : Transaction ID for streaming transactions (may be invalid for non-streaming contexts)  
- : Relation object representing the table where the INSERT occurred
- : TupleTableSlot containing the inserted tuple data
- : Boolean flag indicating whether to use binary format for tuple transmission
- : Bitmapset specifying which columns to include in the replication message

## Dependencies
- Functions called/Symbols referenced:
  - pq_sendbyte
  - LOGICAL_REP_MSG_INSERT
  - pq_sendint32
  - RelationGetRelid
  - logicalrep_write_tuple
  - TransactionIdIsValid
- Called from (representative examples):
  - pgoutput_change

## Notes and Other Information
- Transaction ID is only sent for streaming transactions (when TransactionIdIsValid returns true)
- Uses relation OID as the identifier for the affected table
- The 'N' marker indicates this is a "new tuple" (as opposed to old tuple in UPDATE/DELETE)
- Supports selective column replication through the columns bitmapset parameter
- Part of PostgreSQL's logical replication protocol implementation
- Located in src/backend/replication/logical/proto.c:414-435
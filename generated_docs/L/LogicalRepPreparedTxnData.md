# LogicalRepPreparedTxnData

## Location
src/include/replication/logicalproto.h: 144 - 151

## Overview
LogicalRepPreparedTxnData is a structure that holds protocol information for prepared transactions in PostgreSQL logical replication, specifically used for begin_prepare and prepare messages.

## Definition


## Detailed Description
This structure encapsulates essential information about prepared transactions in the logical replication protocol. It serves as a container for transaction metadata that needs to be transmitted between the publisher and subscriber during two-phase commit operations. The structure is used when handling prepared transaction events, allowing the logical replication system to maintain consistency across distributed transactions.

## Parameters / Member Variables
- : The LSN (Log Sequence Number) at which the transaction was prepared
- : The ending LSN of the prepared transaction
- : The timestamp when the transaction was prepared
- : The transaction ID of the prepared transaction
- : The global identifier string for the prepared transaction, limited by GIDSIZE

## Dependencies
- Types/Constants referenced:
  - XLogRecPtr
  - TimestampTz
  - TransactionId
  - GIDSIZE
- Used by functions:
  - logicalrep_read_begin_prepare
  - logicalrep_read_prepare_common
  - logicalrep_read_prepare
  - logicalrep_read_stream_prepare
  - apply_handle_begin_prepare
  - apply_handle_prepare_internal
  - apply_handle_prepare
  - apply_handle_stream_prepare

## Notes and Other Information
This structure is part of the logical replication protocol implementation and is essential for supporting two-phase commit in logical replication scenarios. It ensures that prepared transaction information is properly communicated between replication participants, maintaining ACID properties in distributed environments.
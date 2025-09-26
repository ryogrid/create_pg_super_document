# logicalrep_read_prepare

## Location
src/backend/replication/logical/proto.c: 239 - 247

## Overview
Reads a transaction PREPARE message from the logical replication stream and populates the provided LogicalRepPreparedTxnData structure with the prepared transaction information.

## Definition


## Detailed Description
This function serves as a wrapper around  to parse a PREPARE message from the logical replication protocol stream. It extracts the prepared transaction data including LSN positions, transaction ID, preparation time, and global identifier (GID) from the binary message format. The function is specifically designed to handle the "prepare" message type in the logical replication protocol, which is part of PostgreSQL's two-phase commit support in logical replication.

## Parameters / Member Variables
- : StringInfo buffer containing the incoming binary message data from the replication stream
- : Pointer to LogicalRepPreparedTxnData structure that will be populated with the parsed prepared transaction information

## Dependencies
- Functions called/Symbols referenced:
  - logicalrep_read_prepare_common
- Called from (representative examples):
  - apply_handle_prepare

## Notes and Other Information
- This function is part of PostgreSQL's logical replication protocol implementation
- It delegates the actual parsing work to  which is shared with stream prepare message handling
- The function is used by logical replication workers to process prepare messages during two-phase commit operations
- Located in src/backend/replication/logical/proto.c:239-247
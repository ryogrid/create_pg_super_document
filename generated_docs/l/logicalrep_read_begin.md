# logicalrep_read_begin

## Location
src/backend/replication/logical/proto.c: 74 - 88

## Overview
Reads and parses a BEGIN message from the logical replication input stream to extract transaction start information.

## Definition


## Detailed Description
This function deserializes a BEGIN message from the logical replication protocol stream. It extracts transaction metadata including the final LSN, commit timestamp, and transaction ID from the input buffer and populates a LogicalRepBeginData structure. The function includes validation to ensure the final_lsn is valid, throwing an error if it's set to InvalidXLogRecPtr.

This is the counterpart to logicalrep_write_begin and is used by logical replication subscribers to process transaction begin messages received from publishers.

## Parameters / Member Variables
- : StringInfo buffer containing the serialized BEGIN message data to be read
- : LogicalRepBeginData structure that will be populated with the extracted transaction information

## Dependencies
- Functions called/Symbols referenced:
  - pq_getmsgint64 (reads a 64-bit integer from the input buffer)
  - pq_getmsgint (reads a 32-bit integer from the input buffer)
  - LogicalRepBeginData (structure type for storing BEGIN message data)
  - InvalidXLogRecPtr (constant for invalid LSN values)
  - elog (logging/error reporting function)
- Called from (representative examples):
  - apply_handle_begin (in the logical replication worker for processing BEGIN messages)

## Notes and Other Information
- This function is part of the logical replication protocol implementation for subscribers
- Includes validation to ensure final_lsn is properly set, preventing processing of invalid transactions
- The function expects the input buffer to contain data in the exact format written by logicalrep_write_begin
- Used by logical replication workers to process transaction boundaries from publishers
- Located in src/backend/replication/logical/proto.c as part of the protocol decoding functions
- Throws an ERROR if the final_lsn field is invalid, which will abort the current transaction
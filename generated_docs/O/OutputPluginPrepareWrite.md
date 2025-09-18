# OutputPluginPrepareWrite

## Location
src/backend/replication/logical/logical.c: 711 - 723

## Overview
Prepares a write operation using the logical decoding context's output routine, ensuring writes are only allowed in appropriate callback contexts.

## Definition
void OutputPluginPrepareWrite(struct LogicalDecodingContext *ctx, bool last_write)

## Detailed Description
This function serves as a controlled entry point for output plugins to prepare write operations during logical decoding. It enforces strict access control by checking that writes are only attempted within valid callback contexts (commit, begin, and change callbacks). When called, it invokes the context's prepare_write function pointer with the current write location, transaction ID, and last_write flag, then marks the context as having a prepared write pending.

The function performs these key operations:
1. Validates that writes are acceptable in the current context
2. Calls the prepare_write callback with location and transaction information
3. Sets the prepared_write flag to track the pending write state

## Parameters / Member Variables
- : LogicalDecodingContext pointer containing the decoding state and output plugin callbacks
- : Boolean flag indicating whether this is the final write in a sequence

## Dependencies
- Functions called/Symbols referenced:
  - LogicalDecodingContext (struct access)
  - elog (for error reporting)
- Called from (representative examples):
  - pgoutput_send_begin
  - pgoutput_commit_txn
  - pgoutput_begin_prepare_txn
  - pgoutput_change
  - pgoutput_truncate
  - pgoutput_message
  - pgoutput_stream_start
  - send_relation_and_attrs

## Notes and Other Information
- This function enforces safety by restricting writes to appropriate callback contexts only
- The prepare_write function pointer is set by the output plugin during initialization
- Used extensively by the pgoutput plugin for preparing various types of logical replication messages
- The last_write parameter allows output plugins to optimize buffer management for the final write in a sequence
- Essential for maintaining proper protocol flow in logical replication output
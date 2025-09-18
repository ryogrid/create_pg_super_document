# OutputPluginWrite

## Location
src/backend/replication/logical/logical.c: 724 - 736

## Overview
Performs an actual write operation using the logical decoding context's output routine, following a previous prepare operation.

## Definition
void OutputPluginWrite(struct LogicalDecodingContext *ctx, bool last_write)

## Detailed Description
This function executes the actual write operation in the logical decoding output process, working in tandem with OutputPluginPrepareWrite to implement a two-phase write protocol. It enforces that a prepare operation must have been called first by checking the prepared_write flag, then invokes the context's write function pointer with the current write location, transaction ID, and last_write flag. After the write completes, it resets the prepared_write flag to indicate the write operation is finished.

The function performs these operations:
1. Validates that OutputPluginPrepareWrite was called first
2. Executes the actual write through the context's write callback
3. Resets the prepared_write flag to complete the write cycle

## Parameters / Member Variables
- : LogicalDecodingContext pointer containing the decoding state and output plugin callbacks
- : Boolean flag indicating whether this is the final write in a sequence

## Dependencies
- Functions called/Symbols referenced:
  - LogicalDecodingContext (struct access)
  - elog (for error reporting)
  - write (function pointer callback)
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
- Must be preceded by OutputPluginPrepareWrite call - enforced through prepared_write flag checking
- The write function pointer is set by the output plugin during initialization
- Used extensively by the pgoutput plugin for executing various types of logical replication message writes
- The two-phase write protocol (prepare/write) allows output plugins to optimize buffer management and ensure proper sequencing
- Essential component of the logical replication output protocol flow
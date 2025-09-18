# OutputPluginWrite

## Location
[src/backend/replication/logical/logical.c:724-736](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L724-L736)

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
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md) (struct access)
  - elog (for error reporting)
  - write (function pointer callback)
- Called from (representative examples):
  - [pgoutput_send_begin](../p/pgoutput_send_begin.md)
  - [pgoutput_commit_txn](../p/pgoutput_commit_txn.md)
  - [pgoutput_begin_prepare_txn](../p/pgoutput_begin_prepare_txn.md)
  - [pgoutput_change](../p/pgoutput_change.md)
  - [pgoutput_truncate](../p/pgoutput_truncate.md)
  - [pgoutput_message](../p/pgoutput_message.md)
  - [pgoutput_stream_start](../p/pgoutput_stream_start.md)
  - [send_relation_and_attrs](../s/send_relation_and_attrs.md)

## Notes and Other Information
- Must be preceded by OutputPluginPrepareWrite call - enforced through prepared_write flag checking
- The write function pointer is set by the output plugin during initialization
- Used extensively by the pgoutput plugin for executing various types of logical replication message writes
- The two-phase write protocol (prepare/write) allows output plugins to optimize buffer management and ensure proper sequencing
- Essential component of the logical replication output protocol flow
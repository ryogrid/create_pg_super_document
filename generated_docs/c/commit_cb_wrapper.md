# commit_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:885-923](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L885-L923)

## Overview
A wrapper function that provides error handling context and state management when executing commit callbacks during logical decoding of transaction commits.

## Definition

```c
static void
commit_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
				  XLogRecPtr commit_lsn)
```
## Detailed Description
The  function serves as a critical wrapper around the actual commit callback in PostgreSQL's logical decoding system. It establishes proper error handling context, sets up output state for the logical decoding context, and then invokes the registered commit callback. This function ensures that any errors during commit processing are properly contextualized and that the logical decoding context is in the correct state for output generation.

The function operates within the reorder buffer framework, which is responsible for reconstructing complete transactions from WAL records in correct commit order. It handles the final stage of transaction processing in logical decoding by preparing the context for output and delegating to the configured output plugin's commit callback.

## Parameters / Member Variables
- : ReorderBuffer instance managing the transaction reordering and callbacks
- : ReorderBufferTXN representing the transaction being committed  
- : XLogRecPtr indicating the WAL location of the commit record

## Dependencies
- Functions called/Symbols referenced:
  - [output_plugin_error_callback](../o/output_plugin_error_callback.md)
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md) (transaction structure)
  - [ReorderBuffer](../R/ReorderBuffer.md) (reorder buffer structure)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md) (decoding context)
  - [LogicalErrorCallbackState](../L/LogicalErrorCallbackState.md) (error callback state)
- Called from (representative examples):
  - [StartupDecodingContext](../S/StartupDecodingContext.md)

## Notes and Other Information
- This is a static function internal to logical.c, used specifically for commit transaction processing
- Establishes error context stack with callback name "commit" for proper error reporting
- Sets context state including accept_writes=true, write_xid, write_location, and end_xact=true
- Uses txn->final_lsn as report_location (beginning of commit record) and txn->end_lsn as write_location (end of record)
- Ensures proper cleanup of error context stack after callback execution
- Part of the logical decoding callback wrapper system that provides consistent error handling across different transaction events
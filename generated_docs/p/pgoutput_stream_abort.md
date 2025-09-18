# pgoutput_stream_abort

## Location
src/backend/replication/pgoutput/pgoutput.c: 1836 - 1868

## Overview
pgoutput_stream_abort handles the abortion of streamed transactions in PostgreSQL logical replication, notifying downstream subscribers to discard the streamed transaction and all its subtransactions.

## Definition


## Detailed Description
pgoutput_stream_abort is a callback function in the pgoutput logical replication output plugin that handles the STREAM ABORT event for transactions that were being streamed but need to be aborted. When a streamed transaction needs to be rolled back, this function notifies downstream subscribers to discard all changes from the transaction and its subtransactions. The function determines the top-level transaction, writes a stream abort message containing transaction IDs and abort timing information, and performs cleanup of relation synchronization cache entries. It ensures the abort occurs outside of any streaming block and includes assertions to validate the transaction state.

## Parameters / Member Variables
- : Logical decoding context containing output plugin state and configuration
- : ReorderBufferTXN structure representing the transaction being aborted
- : XLogRecPtr indicating the LSN where the abort occurred

## Dependencies
- Functions called/Symbols referenced:
  - LogicalDecodingContext
  - ReorderBufferTXN
  - PGOutputData
  - LOGICALREP_STREAM_PARALLEL
  - rbtxn_get_toptxn
  - rbtxn_is_streamed
  - OutputPluginPrepareWrite
  - logicalrep_write_stream_abort
  - OutputPluginWrite
  - cleanup_rel_sync_cache
- Called from (representative examples):
  - _PG_output_plugin_init (registered as callback)

## Notes and Other Information
- This is a static function, only accessible within the pgoutput.c file
- Part of the transaction streaming lifecycle management alongside pgoutput_stream_start and pgoutput_stream_stop
- Includes assertions to ensure streaming is not currently active and that the transaction was previously streamed
- Determines the top-level transaction using rbtxn_get_toptxn to handle subtransaction scenarios properly
- Writes abort information for parallel streaming mode when appropriate
- Performs cleanup of relation synchronization cache to maintain consistency
- Critical for proper transaction rollback handling in logical replication streaming scenarios
- Ensures downstream subscribers can properly handle transaction aborts in streaming replication
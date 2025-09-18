# OutputPluginUpdateProgress

## Location
src/backend/replication/logical/logical.c: 737 - 751

## Overview
Updates progress tracking for logical decoding operations, allowing output plugins to report processing progress to external monitoring systems.

## Definition
void OutputPluginUpdateProgress(struct LogicalDecodingContext *ctx, bool skipped_xact)

## Detailed Description
This function provides a mechanism for output plugins to report progress during logical decoding operations. It acts as an optional callback interface - if the output plugin supports progress tracking (indicated by a non-NULL update_progress function pointer), it calls that function with the current write location, transaction ID, and information about whether a transaction was skipped. This enables monitoring of logical replication progress and can be used for performance analysis, debugging, or user interfaces that display replication status.

The function performs these operations:
1. Checks if the output plugin supports progress tracking
2. If supported, calls the plugin's update_progress callback with current state information
3. If not supported, returns immediately without action

## Parameters / Member Variables
- : LogicalDecodingContext pointer containing the decoding state and output plugin callbacks
- : Boolean flag indicating whether the current transaction was skipped during processing

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md) (struct access)
  - update_progress (function pointer callback)
- Called from (representative examples):
  - [update_progress_txn_cb_wrapper](../u/update_progress_txn_cb_wrapper.md)
  - [pgoutput_commit_txn](../p/pgoutput_commit_txn.md)
  - [pgoutput_prepare_txn](../p/pgoutput_prepare_txn.md)
  - [pgoutput_commit_prepared_txn](../p/pgoutput_commit_prepared_txn.md)
  - [pgoutput_rollback_prepared_txn](../p/pgoutput_rollback_prepared_txn.md)
  - [pgoutput_stream_commit](../p/pgoutput_stream_commit.md)
  - [pgoutput_stream_prepare_txn](../p/pgoutput_stream_prepare_txn.md)

## Notes and Other Information
- Progress tracking is optional - plugins can set update_progress to NULL if not supported
- Primarily used by the pgoutput plugin for reporting logical replication progress
- The skipped_xact parameter helps distinguish between processed and filtered transactions
- Useful for monitoring logical replication lag and throughput
- Can be integrated with PostgreSQL's statistics system or external monitoring tools
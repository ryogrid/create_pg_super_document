# PGOutputData

## Location
src/include/replication/pgoutput.h: 18 - 36

## Overview
PGOutputData is a context structure used by PostgreSQL's logical replication pgoutput plugin to maintain state information during the replication process, storing both client configuration parameters and runtime state.

## Definition


## Detailed Description
PGOutputData serves as the main context structure for PostgreSQL's pgoutput logical replication plugin. This structure encapsulates both the runtime state and client-specified configuration parameters needed for the logical replication output process. The plugin uses this structure to track memory contexts for efficient memory management, maintain streaming transaction state, and store various replication options that control how changes are formatted and transmitted to subscribers.

The structure is designed to be passed between various pgoutput functions to maintain consistent state throughout the replication session. It supports advanced features like streaming transactions, binary protocol, two-phase commit, and message replication.

## Parameters / Member Variables
- : Private memory context used for transient allocations during replication processing
- : Private memory context specifically used for caching data that persists across multiple operations
- : Boolean flag indicating whether currently streaming a chunk of a large transaction
- : Version of the logical replication protocol being used by the client
- : List of publication names that the client has subscribed to
- : List of actual Publication objects corresponding to the publication names
- : Boolean flag indicating whether to use binary format for data transmission
- : Character flag controlling streaming transaction behavior
- : Boolean flag indicating whether to replicate logical decoding messages
- : Boolean flag indicating whether two-phase commit transactions should be supported
- : Boolean flag controlling whether changes without origin should be published

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContext](../M/MemoryContext.md) (for memory management)
  - [List](../L/List.md) (for storing publication information)
- Called from (representative examples):
  - [parse_output_parameters](../p/parse_output_parameters.md)
  - [pgoutput_startup](../p/pgoutput_startup.md)
  - [maybe_send_schema](../m/maybe_send_schema.md)
  - [pgoutput_ensure_entry_cxt](../p/pgoutput_ensure_entry_cxt.md)
  - [pgoutput_row_filter_init](../p/pgoutput_row_filter_init.md)
  - [pgoutput_column_list_init](../p/pgoutput_column_list_init.md)
  - [init_tuple_slot](../i/init_tuple_slot.md)
  - [pgoutput_change](../p/pgoutput_change.md)
  - [pgoutput_truncate](../p/pgoutput_truncate.md)
  - [pgoutput_message](../p/pgoutput_message.md)
  - [pgoutput_origin_filter](../p/pgoutput_origin_filter.md)
  - [pgoutput_stream_start](../p/pgoutput_stream_start.md)
  - [pgoutput_stream_stop](../p/pgoutput_stream_stop.md)
  - [pgoutput_stream_abort](../p/pgoutput_stream_abort.md)
  - [pgoutput_stream_commit](../p/pgoutput_stream_commit.md)
  - [get_rel_sync_entry](../g/get_rel_sync_entry.md)

## Notes and Other Information
- This structure is central to PostgreSQL's logical replication pgoutput plugin implementation
- The dual memory context design (context and cachectx) allows for efficient memory management with different lifetimes for transient vs. cached data
- The streaming-related fields support PostgreSQL's ability to stream large transactions in chunks rather than waiting for transaction commit
- The structure supports various advanced replication features including binary protocol, two-phase commits, and origin filtering
- Located in src/include/replication/pgoutput.h:18-36
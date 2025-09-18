# _PG_output_plugin_init

## Location
src/backend/replication/pgoutput/pgoutput.c: 254 - 282

## Overview
The main initialization function for the pgoutput logical replication output plugin that registers all callback functions required for PostgreSQL logical replication.

## Definition
```c
void _PG_output_plugin_init(OutputPluginCallbacks *cb)
```

## Detailed Description
This function serves as the entry point for the pgoutput output plugin, which is PostgreSQL's built-in logical replication output plugin. It initializes the `OutputPluginCallbacks` structure by assigning all necessary callback functions that handle different aspects of logical replication, including transaction processing, data changes, and streaming operations. The function name follows PostgreSQL's convention for loadable module initialization functions, with the `_PG_` prefix indicating it's a plugin entry point that will be called by the PostgreSQL core when the plugin is loaded.

## Parameters / Member Variables
- `cb`: Pointer to `OutputPluginCallbacks` structure that will be populated with function pointers to handle various logical replication events

## Dependencies
- Functions called/Symbols referenced:
  - [OutputPluginCallbacks](../O/OutputPluginCallbacks.md) (callback structure type)
  - [pgoutput_startup](../p/pgoutput_startup.md) (startup callback)
  - [pgoutput_begin_txn](../p/pgoutput_begin_txn.md) (transaction begin callback)
  - [pgoutput_change](../p/pgoutput_change.md) (data change callback) 
  - [pgoutput_truncate](../p/pgoutput_truncate.md) (truncate callback)
  - [pgoutput_message](../p/pgoutput_message.md) (message callback)
  - [pgoutput_commit_txn](../p/pgoutput_commit_txn.md) (transaction commit callback)
  - [pgoutput_begin_prepare_txn](../p/pgoutput_begin_prepare_txn.md) (prepare transaction begin callback)
  - [pgoutput_prepare_txn](../p/pgoutput_prepare_txn.md) (prepare transaction callback)
  - [pgoutput_commit_prepared_txn](../p/pgoutput_commit_prepared_txn.md) (prepared transaction commit callback)
  - [pgoutput_rollback_prepared_txn](../p/pgoutput_rollback_prepared_txn.md) (prepared transaction rollback callback)
  - [pgoutput_origin_filter](../p/pgoutput_origin_filter.md) (origin filtering callback)
  - [pgoutput_shutdown](../p/pgoutput_shutdown.md) (shutdown callback)
  - [pgoutput_stream_start](../p/pgoutput_stream_start.md) (streaming start callback)
  - [pgoutput_stream_stop](../p/pgoutput_stream_stop.md) (streaming stop callback)
  - [pgoutput_stream_abort](../p/pgoutput_stream_abort.md) (streaming abort callback)
  - [pgoutput_stream_commit](../p/pgoutput_stream_commit.md) (streaming commit callback)
  - [pgoutput_stream_prepare_txn](../p/pgoutput_stream_prepare_txn.md) (streaming prepare transaction callback)
- Called from:
  - PostgreSQL core plugin loader (automatically invoked when plugin is loaded)

## Notes and Other Information
- This function is automatically called by PostgreSQL when the pgoutput plugin is loaded
- The function sets up callbacks for both regular logical replication and streaming (large transaction) scenarios
- Two-phase commit support is included for distributed transactions
- The same change and message callbacks are reused for both regular and streaming operations
- This is the only externally visible function in the pgoutput plugin module
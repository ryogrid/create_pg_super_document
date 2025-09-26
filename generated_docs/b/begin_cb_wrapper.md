# begin_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:854-884](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L854-L884)

## Overview
begin_cb_wrapper is a static callback wrapper function that handles transaction begin events in logical replication by calling the output plugin's begin callback with proper error handling and context management.

## Definition
```c
static void begin_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn)
```

## Detailed Description
This function serves as a callback wrapper for the ReorderBuffer system, specifically handling transaction begin events during logical replication decoding. It prepares the logical decoding context for transaction processing, establishes error context tracking, and then invokes the output plugin's begin callback. Unlike the startup and shutdown wrappers, this callback has an associated LSN (from the transaction's first_lsn) and enables write operations.

The function is part of the ReorderBuffer callback infrastructure, which processes WAL records in transaction order and delivers them to output plugins. It ensures that the decoding context is properly configured for the beginning of a new transaction and provides detailed error reporting if the plugin callback fails.

## Parameters / Member Variables
- `cache`: Pointer to ReorderBuffer containing the logical decoding context in private_data
- `txn`: Pointer to ReorderBufferTXN structure representing the transaction being started

## Dependencies
- Functions called/Symbols referenced:
  - ReorderBufferTXN
  - ReorderBuffer
  - LogicalDecodingContext
  - LogicalErrorCallbackState
  - output_plugin_error_callback
  - callback
- Called from (representative examples):
  - StartupDecodingContext

## Notes and Other Information
- The function asserts that fast_forward mode is disabled during transaction processing
- Sets ctx->accept_writes to true, enabling output plugin to generate output during transaction
- Configures write context with transaction XID and LSN information (ctx->write_xid, ctx->write_location)
- Uses transaction's first_lsn as the report_location for error context
- Static function used as a callback within the ReorderBuffer infrastructure
- Part of the plugin callback wrapper ecosystem that provides consistent error handling
- Critical for initiating logical replication transaction processing
- The error context includes LSN information unlike startup/shutdown wrappers, enabling precise error location reporting
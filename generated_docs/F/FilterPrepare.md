# FilterPrepare

## Location
src/backend/replication/logical/decode.c: 563 - 585

## Overview
The `FilterPrepare` function determines whether a PREPARE transaction should be skipped and sent as a regular commit later, based on two-phase transaction configuration and output plugin callbacks.

## Definition
```c
static inline bool FilterPrepare(LogicalDecodingContext *ctx, TransactionId xid, const char *gid)
```

## Detailed Description
This function implements the filtering logic for two-phase transactions during logical decoding. It serves as a decision point to determine whether a prepared transaction should be processed immediately at PREPARE time or deferred until COMMIT PREPARED. The function first checks if two-phase decoding is enabled in the context, then consults the output plugin's filter callback if available.

The filtering process follows a hierarchical approach:
1. **Two-phase disabled**: If `ctx->twophase` is false, all prepared transactions are filtered out
2. **No filter callback**: If no `filter_prepare_cb` is provided, all prepared transactions are allowed through
3. **Plugin decision**: If a callback exists, it determines the filtering based on plugin-specific logic

## Parameters / Member Variables
- `ctx`: LogicalDecodingContext containing the two-phase configuration and output plugin callbacks
- `xid`: TransactionId of the transaction being prepared
- `gid`: Global identifier string for the prepared transaction

## Dependencies
- Functions called/Symbols referenced:
  - filter_prepare_cb_wrapper
- Called from (representative examples):
  - xact_decode (multiple calls at lines 239, 267, 325)

## Notes and Other Information
- Returns `true` if the PREPARE should be skipped (filtered out)
- Returns `false` if the PREPARE should be processed immediately
- The function is marked `static inline` for performance optimization
- When two-phase decoding is disabled, transactions are processed as regular commits at COMMIT PREPARED time
- The filter callback allows output plugins to implement custom logic for selective two-phase transaction processing
- This filtering mechanism provides flexibility for different replication scenarios and plugin requirements
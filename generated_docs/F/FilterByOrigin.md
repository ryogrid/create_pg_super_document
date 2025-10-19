# FilterByOrigin

## Location
[src/backend/replication/logical/decode.c:586-597](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/decode.c#L586-L597)

## Overview
The `FilterByOrigin` function determines whether changes from a specific replication origin should be filtered out during logical decoding based on the output plugin's origin filtering callback.

## Definition
```c
static inline bool FilterByOrigin(LogicalDecodingContext *ctx, RepOriginId origin_id)
```

## Detailed Description
This function provides origin-based filtering for logical replication, allowing output plugins to selectively exclude changes based on their replication origin. This is particularly useful in multi-master replication scenarios where you want to avoid replicating changes that originated from certain nodes to prevent infinite loops or unwanted cascading.

The function implements a simple delegation pattern:
- If no origin filter callback is registered in the output plugin, it allows all changes through (returns false)
- If a callback is present, it delegates the filtering decision to the plugin-specific logic

This filtering mechanism is commonly used to:
- Prevent replication loops in multi-master setups
- Exclude changes from specific replication origins
- Implement selective replication based on origin policies

## Parameters / Member Variables
- `ctx`: LogicalDecodingContext containing the output plugin callbacks and decoding state
- `origin_id`: RepOriginId identifying the replication origin of the change being processed

## Dependencies
- Functions called/Symbols referenced:
  - [filter_by_origin_cb_wrapper](../f/filter_by_origin_cb_wrapper.md)
- Called from (representative examples):
  - [logicalmsg_decode](../l/logicalmsg_decode.md)
  - [DecodeInsert](../D/DecodeInsert.md)
  - [DecodeUpdate](../D/DecodeUpdate.md)
  - [DecodeDelete](../D/DecodeDelete.md)
  - [DecodeTruncate](../D/DecodeTruncate.md)
  - [DecodeMultiInsert](../D/DecodeMultiInsert.md)
  - [DecodeSpecConfirm](../D/DecodeSpecConfirm.md)
  - [DecodeTXNNeedSkip](../D/DecodeTXNNeedSkip.md)

## Notes and Other Information
- Returns `true` if the change should be filtered out (skipped)
- Returns `false` if the change should be processed
- The function is marked `static inline` for performance optimization
- Used extensively throughout the decode functions to filter changes at various levels
- Essential for preventing replication loops in complex replication topologies
- The origin filtering is applied at the individual change level, providing fine-grained control
- When no filter callback is provided, the default behavior is to process all changes regardless of origin

## Simplified Source

```c
static inline bool FilterByOrigin(LogicalDecodingContext *ctx, RepOriginId origin_id) {
    // If no origin filter callback is set, allow all changes through
    if (ctx->callbacks.filter_by_origin_cb == NULL)
        return false;

    // Delegate filtering decision to the plugin-specific callback
    return filter_by_origin_cb_wrapper(ctx, origin_id);
}
```
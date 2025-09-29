# SnapBuildXactNeedsSkip

## Location
[src/backend/replication/logical/snapbuild.c:443-454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L443-L454)

## Overview
Determines whether the contents of a transaction ending at a specified LSN should be skipped during logical decoding based on the snapshot builder's decoding threshold.

## Definition
```c
bool SnapBuildXactNeedsSkip(SnapBuild *builder, XLogRecPtr ptr)
```

## Detailed Description
This function implements a simple but critical decision logic for logical replication: it determines whether a transaction that ends at the given LSN should be decoded or skipped. The decision is based on comparing the transaction's end LSN with the `start_decoding_at` field of the snapshot builder. Transactions that end before the decoding start point are skipped, which is essential for maintaining consistency during logical replication startup and for avoiding decoding of transactions that were already processed or are not relevant to the current replication session.

## Parameters / Member Variables
- `builder`: Pointer to the SnapBuild structure containing the decoding configuration
- `ptr`: The XLogRecPtr (LSN) where the transaction ends

## Dependencies
- Functions called/Symbols referenced:
  - [SnapBuild](SnapBuild.md) (struct type)
- Called from (representative examples):
  - [logicalmsg_decode](../l/logicalmsg_decode.md)
  - [DecodeTXNNeedSkip](../D/DecodeTXNNeedSkip.md)
  - [AssertTXNLsnOrder](../A/AssertTXNLsnOrder.md)
  - [ReorderBufferCanStartStreaming](../R/ReorderBufferCanStartStreaming.md)

## Notes and Other Information
- Returns true if the transaction should be skipped (ptr < start_decoding_at)
- Essential for logical replication consistency and performance
- Used throughout the logical decoding infrastructure to filter relevant transactions
- Simple comparison function but critical for proper logical replication behavior
- Located in src/backend/replication/logical/snapbuild.c:443-454

## Simplified Source

```c
bool
SnapBuildXactNeedsSkip(SnapBuild *builder, XLogRecPtr ptr)
{
    // Skip transaction if it ends before we started decoding
    return ptr < builder->start_decoding_at;
}
```
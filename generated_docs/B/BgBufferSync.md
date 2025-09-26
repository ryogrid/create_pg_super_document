# BgBufferSync

## Location
[src/backend/storage/buffer/bufmgr.c:3177-3474](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L3177-L3474)

## Overview
BgBufferSync periodically writes out dirty buffers in the background, implementing PostgreSQL's LRU-based buffer cleaning strategy with sophisticated allocation rate tracking and adaptive scanning.

## Definition
```c
bool BgBufferSync(WritebackContext *wb_context)
```

## Detailed Description
BgBufferSync is called periodically by the background writer process to proactively clean dirty buffers before they are needed for new allocations. It implements a sophisticated algorithm that tracks buffer allocation rates, maintains moving averages of allocation patterns, and estimates the density of reusable buffers. The function operates by scanning ahead of the strategy point (freelist clock sweep), cleaning dirty buffers to stay ahead of allocation demand. It uses adaptive algorithms to balance between being too aggressive (wasting I/O) and too conservative (causing allocation delays). The function can enter hibernation mode when there's little allocation activity and the strategy sweep has been "lapped".

## Parameters / Member Variables
- `wb_context`: Writeback context for batching and managing I/O operations
- Returns: `bool` indicating whether the bgwriter should hibernate (true if strategy clock was lapped and no recent allocations)

## Dependencies
- Functions called/Symbols referenced:
  - StrategySyncStart
  - SyncOneBuffer
  - BUF_WRITTEN
  - BUF_REUSABLE
- Called from (representative examples):
  - BackgroundWriterMain
  - RelationGetNumberOfBlocks

## Notes and Other Information
- Implements LRU-based buffer cleaning strategy that stays ahead of allocation demand
- Uses sophisticated moving averages to track allocation rates and buffer density
- Can be disabled by setting bgwriter_lru_maxpages to 0
- Includes adaptive algorithms to handle varying workload patterns
- Supports hibernation mode when there's minimal allocation activity
- Tracks multiple statistics including smoothed allocation rates and buffer density estimates
- Balances between staying ahead of allocations and avoiding excessive I/O
- Uses configurable parameters like bgwriter_lru_multiplier for tuning aggressiveness
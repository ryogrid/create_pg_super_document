# ResetDecoder

## Location
[src/backend/access/transam/xlogreader.c:1605-1638](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogreader.c#L1605-L1638)

## Overview
ResetDecoder is a private function that resets the XLogReaderState's internal decoding state, clearing all decoded records and buffers when moving to a new read position.

## Definition

```c
struct. */
	size += offsetof(DecodedXLogRecord, blocks[0]);
```
## Detailed Description
ResetDecoder provides a clean slate for XLog reading operations by completely resetting the decoder's internal state. The function clears the decoded record queue, freeing any oversized records that were dynamically allocated, resets decode buffers to their initial empty state, and clears any pending error conditions. This ensures that when the XLogReader moves to a new position, there are no stale decoded records or error states that could interfere with subsequent operations.

## Parameters / Member Variables
- `state`: XLogReaderState containing the decoding context to be reset

## Dependencies
- Functions called/Symbols referenced:
  - [DecodedXLogRecord](../D/DecodedXLogRecord.md) (structure type)
  - [pfree](../p/pfree.md) (for freeing oversized records)
- Called from (representative examples):
  - [XLogBeginRead](../X/XLogBeginRead.md)

## Notes and Other Information
- This is a private static function within xlogreader.c, not exposed to external modules
- Handles memory management by freeing only oversized records (normal records are managed in a pre-allocated pool)
- Resets both the decode queue and decode buffers to ensure consistent state
- Clears error message buffer and deferred error flags
- Essential for maintaining clean state when seeking to different positions in WAL
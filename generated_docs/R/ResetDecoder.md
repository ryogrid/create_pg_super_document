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

## Simplified Source

```c
// Simplified version of ResetDecoder
static void ResetDecoder(XLogReaderState *state)
{
    DecodedXLogRecord *current_record;

    // Core logic step 1: Clear the decoded record queue
    while ((current_record = state->decode_queue_head) != NULL) {
        state->decode_queue_head = current_record->next;
        if (current_record->oversized) {
            pfree(current_record);  // Free oversized records
        }
    }

    // Core logic step 2: Reset queue pointers
    state->decode_queue_tail = NULL;
    state->decode_queue_head = NULL;
    state->record = NULL;

    // Core logic step 3: Reset decode buffers to empty state
    state->decode_buffer_tail = state->decode_buffer;
    state->decode_buffer_head = state->decode_buffer;

    // Core logic step 4: Clear error state
    state->errormsg_buf[0] = '\0';
    state->errormsg_deferred = false;
}
```

Key simplifications made:
- Renamed loop variable from `r` to `current_record` for clarity
- Added descriptive comments for each major step
- Preserved essential memory management logic for oversized records
- Maintained all critical state resets
- Organized operations into logical groupings with clear comments
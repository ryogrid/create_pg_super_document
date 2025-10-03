# AppendIncrementalManifestData

## Location
[src/backend/backup/basebackup_incremental.c:196-228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_incremental.c#L196-L228)

## Overview
Appends received backup manifest data chunks to an incremental backup buffer, managing memory and performing incremental JSON parsing when buffer limits are reached.

## Definition

```c
void
AppendIncrementalManifestData(IncrementalBackupInfo *ib, const char *data,
							  int len)
```
## Detailed Description
This function is called during incremental backup setup to process chunks of backup manifest data received from the client. It maintains an internal buffer that accumulates the manifest data, and when the buffer reaches certain size thresholds, it triggers incremental JSON parsing to process the accumulated data while retaining a minimum chunk for continuity.

The function implements a streaming parser approach where:
- Data is accumulated in a buffer (ib->buf)
- When the buffer exceeds MAX_CHUNK size, incremental parsing is triggered
- A minimum chunk (MIN_CHUNK) is always retained for the next parsing cycle
- Memory management is handled by switching to the incremental backup's memory context

## Parameters / Member Variables
- `*ib`: IncrementalBackupInfo structure containing the incremental backup state and buffer
- `*data`: Pointer to the manifest data chunk to append
- `len`: Length of the data chunk in bytes
## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [json_parse_manifest_incremental_chunk](../j/json_parse_manifest_incremental_chunk.md)
  - memmove
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md)
- Types referenced:
  - [IncrementalBackupInfo](../I/IncrementalBackupInfo.md)
  - MIN_CHUNK, MAX_CHUNK (constants)
- Called from:
  - [HandleUploadManifestPacket](../H/HandleUploadManifestPacket.md) (src/backend/replication/walsender.c:793)

## Notes and Other Information
- This function is designed to handle manifest data in streaming fashion to avoid memory exhaustion with large manifests
- The incremental parsing mechanism ensures that JSON parsing can proceed without requiring the entire manifest to be buffered in memory
- Memory context switching ensures proper memory management within the incremental backup subsystem
- The MIN_CHUNK retention strategy prevents JSON parsing boundary issues by always keeping some data available for the next parsing cycle

## Simplified Source

```c
// Simplified version of AppendIncrementalManifestData
void AppendIncrementalManifestData(IncrementalBackupInfo *ib, const char *data, int len) {
    // Switch to incremental backup memory context
    MemoryContext oldcontext = MemoryContextSwitchTo(ib->mcxt);

    // Check if buffer is getting too large and needs incremental parsing
    if (ib->buf.len > MIN_CHUNK && ib->buf.len + len > MAX_CHUNK) {
        // Parse most of the buffer, keeping MIN_CHUNK for continuity
        json_parse_manifest_incremental_chunk(ib->inc_state, ib->buf.data,
                                             ib->buf.len - MIN_CHUNK, false);

        // Shift remaining data to beginning of buffer
        memmove(ib->buf.data, ib->buf.data + (ib->buf.len - MIN_CHUNK), MIN_CHUNK + 1);
        ib->buf.len = MIN_CHUNK;
    }

    // Append new data to buffer
    appendBinaryStringInfo(&ib->buf, data, len);

    // Restore previous memory context
    MemoryContextSwitchTo(oldcontext);
}
```

Key simplifications made:
- Removed detailed comments and consolidated into clear step descriptions
- Simplified variable declarations by combining declaration and assignment
- Made the buffer size logic more readable with clear comments
- Abstracted the memmove operation with a descriptive comment
- Maintained all essential functionality while improving readability
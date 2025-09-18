# bbstreamer_lz4_decompressor_finalize

## Location
[src/bin/pg_basebackup/bbstreamer_lz4.c:390-411](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_lz4.c#L390-L411)

## Overview
Handles end-of-stream processing for LZ4 decompression by forwarding any remaining buffered data to the next streamer and finalizing the entire processing chain.

## Definition
```c
static void bbstreamer_lz4_decompressor_finalize(bbstreamer *streamer)
```

## Detailed Description
This function performs cleanup and finalization when the LZ4-compressed backup stream reaches its end. It ensures that any partially-filled output buffer is forwarded to the next streamer in the processing chain, preventing data loss of the final decompressed chunk. After forwarding the remaining buffer contents, it calls the finalize function of the next streamer to properly terminate the entire processing pipeline.

The function uses the BBSTREAMER_UNKNOWN context when forwarding final data since the specific archive context may not be determinable at stream end. The member parameter is passed as NULL since there is no specific archive member associated with finalization.

## Parameters / Member Variables
- `streamer`: Pointer to the base bbstreamer object, internally cast to bbstreamer_lz4_frame to access the decompressor state

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer_content](bbstreamer_content.md) (forwards remaining buffered data to next streamer)
  - [bbstreamer_finalize](bbstreamer_finalize.md) (finalizes the next streamer in the chain)
  - BBSTREAMER_UNKNOWN (archive context constant for unknown/unspecified context)
- Called from (representative examples):
  - Referenced indirectly through bbstreamer function pointer mechanism during stream finalization

## Notes and Other Information
- This is a static function used internally within the LZ4 streaming decompressor implementation
- Critical for ensuring no decompressed data is lost when the input stream ends
- Must be called exactly once at the end of stream processing to properly clean up the processing pipeline
- The function assumes that any pending data in the output buffer represents the final portion of decompressed content
- Forwarded data uses the maximum buffer length rather than tracking partial fills, relying on the next streamer to handle appropriate data boundaries
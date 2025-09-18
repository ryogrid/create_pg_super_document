# Zstd_close

## Location
[src/bin/pg_dump/compress_zstd.c:435-495](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_zstd.c#L435-L495)

## Overview
Closes a Zstd-compressed file handle, finalizing compression/decompression and cleaning up resources.

## Definition
```c
static bool Zstd_close(CompressFileHandle *CFH)
```

## Detailed Description
This function handles the proper closure of a Zstd-compressed file stream. It performs different cleanup operations depending on whether the stream was used for compression or decompression:

For compression streams:
- Finalizes the compression by calling ZSTD_compressStream2 with ZSTD_e_end flag
- Flushes any remaining compressed data to the output file
- Frees the compression stream and output buffer

For decompression streams:
- Frees the decompression stream and input buffer

The function ensures proper resource cleanup by freeing all allocated memory and closing the underlying file handle. It returns a boolean indicating whether the operation completed successfully.

## Parameters / Member Variables
- `CFH`: Compressed file handle to close

## Dependencies
- Functions called/Symbols referenced:
  - [ZstdCompressorState](ZstdCompressorState.md)
  - ZSTD_compressStream2
  - ZSTD_isError
  - ZSTD_getErrorName
  - ZSTD_freeCStream
  - ZSTD_freeDStream
  - fwrite
  - fclose
  - strerror
  - [pg_free](../p/pg_free.md)
  - unconstify
- Called from (representative examples):
  - [InitCompressFileHandleZstd](../I/InitCompressFileHandleZstd.md) (as part of function pointer assignment)

## Notes and Other Information
- This is a static function within the Zstd compression module
- Returns true on successful closure, false on error
- Handles both compression and decompression stream cleanup
- Sets appropriate error messages in zstdcs->zstderror on failure
- Properly manages errno for file I/O operations
- Frees all allocated resources including streams, buffers, and the state structure
- The function uses unconstify() to safely free const input buffer data
- Part of the compression abstraction layer ensuring consistent cleanup across different compression formats
# ReadDataFromArchiveNone

## Location
[src/bin/pg_dump/compress_none.c:30-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_none.c#L30-L48)

## Overview
Reads data from an archive when no compression is used, implementing the compressor API for uncompressed data streams in pg_dump.

## Definition


## Detailed Description
This function implements the data reading functionality for the "none" compression method in pg_dump. It reads data from the archive using the compressor state's read function and writes it directly to the archive handle without any decompression processing. The function operates in a simple loop, continuously reading chunks of data until no more data is available, then immediately writing each chunk to the archive output.

## Parameters / Member Variables
- : Archive handle containing the archive context and output methods
- : Compressor state structure containing the read function pointer and other compression-related state

## Dependencies
- Functions called/Symbols referenced:
  - [CompressorState](../C/CompressorState.md) (struct type)
  - DEFAULT_IO_BUFFER_SIZE (constant)
  - pg_malloc (memory allocation)
  - [ahwrite](../a/ahwrite.md) (archive write function)
- Called from (representative examples):
  - [InitCompressorNone](../I/InitCompressorNone.md)

## Notes and Other Information
- This function is part of the compressor API for handling uncompressed data streams
- Uses a default I/O buffer size for efficient data transfer
- Memory is properly allocated and freed for the transfer buffer
- The function continues reading until the read function returns 0 (no more data)
- Located in src/bin/pg_dump/compress_none.c:30-48
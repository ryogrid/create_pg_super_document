# WriteDataToArchiveNone

## Location
src/bin/pg_dump/compress_none.c: 49 - 55

## Overview
Writes data to an archive when no compression is used, implementing the compressor API for uncompressed data streams in pg_dump.

## Definition
static void WriteDataToArchiveNone(ArchiveHandle *AH, CompressorState *cs, const void *data, size_t dLen)

## Detailed Description
This function implements the data writing functionality for the "none" compression method in pg_dump. It directly forwards the write operation to the compressor state's write function without performing any compression processing. This is the simplest implementation of the compressor API, essentially acting as a pass-through for data that should remain uncompressed.

## Parameters / Member Variables
- : Archive handle containing the archive context and output methods
- : Compressor state structure containing the write function pointer and other compression-related state
- : Pointer to the data buffer to be written
- : Size of the data to be written in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [CompressorState](../C/CompressorState.md) (struct type)
- Called from (representative examples):
  - [InitCompressorNone](../I/InitCompressorNone.md)

## Notes and Other Information
- This function is part of the compressor API for handling uncompressed data streams
- Simply delegates to the write function stored in the compressor state
- No data transformation or compression is performed
- Located in src/bin/pg_dump/compress_none.c:49-55
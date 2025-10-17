# InitCompressorNone

## Location
[src/bin/pg_dump/compress_none.c:66-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_none.c#L66-L86)

## Overview
Initializes the compressor state for the "none" compression method, setting up function pointers for uncompressed data handling in pg_dump.

## Definition
void InitCompressorNone(CompressorState *cs, const pg_compress_specification compression_spec)

## Detailed Description
This function serves as the public interface for initializing the "none" compression implementation in pg_dump. It sets up the compressor state structure by assigning the appropriate function pointers for reading, writing, and finalizing data operations when no compression is desired. The function configures the compressor state to use the none-specific implementations of the compressor API, effectively creating a pass-through compression layer.

## Parameters / Member Variables
- : Compressor state structure to be initialized with function pointers and configuration
- : Compression specification containing configuration details for the compression method

## Dependencies
- Functions called/Symbols referenced:
  - [CompressorState](../C/CompressorState.md) (struct type)
  - [pg_compress_specification](../p/pg_compress_specification.md) (struct type)
  - [ReadDataFromArchiveNone](../R/ReadDataFromArchiveNone.md) (function pointer assignment)
  - [WriteDataToArchiveNone](../W/WriteDataToArchiveNone.md) (function pointer assignment)  
  - [EndCompressorNone](../E/EndCompressorNone.md) (function pointer assignment)
- Called from (representative examples):
  - [AllocateCompressor](../A/AllocateCompressor.md)

## Notes and Other Information
- This function is part of the public interface for the none compression module
- Sets up a complete compressor API implementation for uncompressed data
- The compression_spec parameter is stored but not otherwise used in the none implementation
- All function pointers are set to none-specific implementations that perform no compression
- Located in src/bin/pg_dump/compress_none.c:66-86

## Simplified Source

```c
void
InitCompressorNone(CompressorState *cs,
                   const pg_compress_specification compression_spec)
{
    // Set up function pointers for no-compression operations
    cs->readData = ReadDataFromArchiveNone;
    cs->writeData = WriteDataToArchiveNone;
    cs->end = EndCompressorNone;

    // Store compression specification
    cs->compression_spec = compression_spec;
}
```
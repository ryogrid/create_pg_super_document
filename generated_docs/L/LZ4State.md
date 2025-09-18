# LZ4State

## Location
[src/bin/pg_dump/compress_lz4.c:39-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_lz4.c#L39-L89)

## Overview
LZ4State is a comprehensive state structure used by both the Compressor and Stream APIs in PostgreSQL's pg_dump utility for LZ4 compression and decompression operations.

## Definition


## Detailed Description
LZ4State serves as the central state management structure for LZ4 compression operations in pg_dump. It supports both streaming and direct compression APIs, maintaining all necessary context for compression/decompression operations including file handles, LZ4 library contexts, buffers, and operational flags. The structure is designed to handle lazy initialization, distinguish between compression and decompression modes, and manage data overflow scenarios efficiently.

## Parameters / Member Variables
- : File pointer used by the Stream API to track the file stream being processed
- : LZ4F_preferences_t structure containing LZ4 compression preferences and settings
- : LZ4F_compressionContext_t context for LZ4 compression operations
- : LZ4F_decompressionContext_t context for LZ4 decompression operations
- : Boolean flag used by Stream API for lazy initialization tracking
- : Boolean flag used by Stream API to distinguish between compression and decompression operations
- : Boolean flag used by Compressor API to mark if compression headers need to be written after initialization
- : Size of the main data buffer
- : Main data buffer for storing compressed/uncompressed data
- : Allocated length of the overflow buffer
- : Current length of data in the overflow buffer
- : Overflow buffer used by Stream API to store uncompressed data not yet consumed by the caller
- : Length of compressed data currently stored in the buffer (used by both APIs)
- : Error code tracking for both APIs to maintain error state

## Dependencies
- Functions called/Symbols referenced:
  - LZ4F_preferences_t
  - LZ4F_compressionContext_t
  - LZ4F_decompressionContext_t
  - FILE
- Called from (representative examples):
  - [LZ4State_compression_init](LZ4State_compression_init.md)
  - [WriteDataToArchiveLZ4](../W/WriteDataToArchiveLZ4.md)
  - [EndCompressorLZ4](../E/EndCompressorLZ4.md)
  - [LZ4Stream_eof](LZ4Stream_eof.md)
  - [LZ4Stream_init](LZ4Stream_init.md)
  - [LZ4Stream_read_overflow](LZ4Stream_read_overflow.md)
  - [LZ4Stream_write](LZ4Stream_write.md)
  - [LZ4Stream_read](LZ4Stream_read.md)
  - [LZ4Stream_open](LZ4Stream_open.md)
  - [LZ4Stream_close](LZ4Stream_close.md)

## Notes and Other Information
- The structure is designed to be versatile, supporting both streaming operations (through file pointers) and direct buffer operations
- Lazy initialization is supported through the  flag to optimize performance
- The overflow buffer mechanism allows for efficient handling of partial reads in streaming scenarios
- Error state is maintained centrally through the  member for consistent error handling across both APIs
- The structure is located in src/bin/pg_dump/compress_lz4.c and is specific to pg_dump's LZ4 compression implementation
# InitCompressorLZ4

## Location
[src/bin/pg_dump/compress_lz4.c:797-803](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_lz4.c#L797-L803)

## Overview
Initializes the LZ4 compressor state for use with PostgreSQL's pg_dump utility, setting up the compression context and function pointers for LZ4-based archive compression operations.

## Definition


## Detailed Description
InitCompressorLZ4 serves as the initialization function for LZ4 compression within PostgreSQL's backup and restore infrastructure. The function has two different implementations depending on whether LZ4 support is compiled into the build:

**When LZ4 is enabled (USE_LZ4 defined):**
- Sets up the CompressorState structure with LZ4-specific function pointers for reading, writing, and cleanup operations
- Initializes the LZ4 compression context and buffers for write operations
- Configures compression level based on the provided specification
- Prepares the LZ4 frame header for later writing

**When LZ4 is disabled (USE_LZ4 not defined):**
- Simply calls pg_fatal() to report that LZ4 compression is not supported in this build

The function handles both read and write scenarios differently - for read operations, it only sets up function pointers since the entire input is available, while for write operations it performs full state initialization including buffer allocation and compression context setup.

## Parameters / Member Variables
- `cs`: Pointer to the CompressorState structure that will be configured for LZ4 operations
- `compression_spec`: Specification containing compression parameters such as compression level

## Dependencies
- Functions called/Symbols referenced:
  - [ReadDataFromArchiveLZ4](../R/ReadDataFromArchiveLZ4.md) (assigned as readData function)
  - [WriteDataToArchiveLZ4](../W/WriteDataToArchiveLZ4.md) (assigned as writeData function)
  - [EndCompressorLZ4](../E/EndCompressorLZ4.md) (assigned as end function)
  - pg_malloc0 (for state allocation)
  - [LZ4State_compression_init](../L/LZ4State_compression_init.md) (for initializing LZ4 compression state)
  - [pg_fatal](../p/pg_fatal.md) (for error reporting)
  - LZ4F_getErrorName (for error message formatting)
- Called from (representative examples):
  - [AllocateCompressor](../A/AllocateCompressor.md) (in compress_io.c)

## Notes and Other Information
- This function is part of the public interface for LZ4 compression in pg_dump
- The function's behavior is conditional on compile-time LZ4 library availability
- For write operations, the function sets up a deferred header writing mechanism (needs_header_flush flag)
- Read operations are stateless and don't require persistent state between calls
- The function is designed to integrate with PostgreSQL's pluggable compression architecture
- Error handling uses PostgreSQL's standard pg_fatal() mechanism for unrecoverable errors
- The LZ4 compression level can be customized through the compression_spec parameter
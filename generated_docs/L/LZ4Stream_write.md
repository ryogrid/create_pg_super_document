# LZ4Stream_write

## Location
src/bin/pg_dump/compress_lz4.c: 573 - 609

## Overview
Compresses data from a source buffer and writes it to the LZ4 compressed stream in chunks, handling lazy initialization and error management.

## Definition
```c
static void
LZ4Stream_write(const void *ptr, size_t size, CompressFileHandle *CFH)
```

## Detailed Description
This static function serves as the primary interface for writing compressed data to an LZ4 stream in PostgreSQL's pg_dump utility. It performs lazy initialization by calling LZ4Stream_init() with compression mode enabled, then processes the input data in chunks no larger than DEFAULT_IO_BUFFER_SIZE to manage memory usage efficiently. For each chunk, it uses the LZ4F_compressUpdate() library function to compress the data into the internal buffer, then writes the compressed output to the file stream using fwrite(). The function includes comprehensive error handling with pg_fatal() calls that terminate the program on any compression or I/O errors, ensuring data integrity is maintained. The chunked approach allows it to handle arbitrarily large input sizes while maintaining consistent memory usage.

## Parameters / Member Variables
- `ptr`: Pointer to the source data buffer to be compressed
- `size`: Number of bytes to read from the source buffer and compress
- `CFH`: Pointer to CompressFileHandle structure containing the LZ4State private data and file stream

## Dependencies
- Functions called/Symbols referenced:
  - LZ4Stream_init (internal initialization function)
  - LZ4F_compressUpdate (LZ4 library compression function)
  - LZ4F_isError (LZ4 library error checking function)
  - LZ4F_getErrorName (LZ4 library error message function)
  - fwrite (standard C library I/O function)
  - pg_fatal (PostgreSQL fatal error function - terminates program)
  - Min (PostgreSQL macro for minimum value)
- Constants referenced:
  - DEFAULT_IO_BUFFER_SIZE (chunk size limit for processing)
  - ENOSPC (system error code for no space)
- Types referenced:
  - CompressFileHandle
  - LZ4State

## Notes and Other Information
- This function uses pg_fatal() for error handling, meaning any error (compression failure or I/O error) will terminate the entire program - this is appropriate for pg_dump where data integrity is critical
- The chunked processing approach (using DEFAULT_IO_BUFFER_SIZE) ensures consistent memory usage regardless of input data size
- Lazy initialization means the LZ4 compression context and buffers are only set up when the first write operation occurs
- The function advances the source pointer (ptr) after processing each chunk to ensure all data is processed
- Unlike read operations, write operations don't require overflow buffer management since compression typically reduces data size
- The function handles partial writes by checking that fwrite() returns the expected number of bytes written
- Error handling distinguishes between LZ4 compression errors and I/O errors, providing appropriate error messages for each case
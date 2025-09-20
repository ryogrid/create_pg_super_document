# InitCompressFileHandleZstd

## Location
[src/bin/pg_dump/compress_zstd.c:559-577](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_zstd.c#L559-L577)

## Overview
Initializes a CompressFileHandle structure with zstd-specific function pointers and compression settings for use in PostgreSQL's pg_dump utility.

## Definition

```c
void
InitCompressFileHandleZstd(CompressFileHandle *CFH,
						   const pg_compress_specification compression_spec)
```
## Detailed Description
This function serves as the initialization routine for the zstd compression backend in pg_dump. It configures a CompressFileHandle by assigning zstd-specific implementations to all the function pointers required for file I/O operations. The function sets up the complete interface for reading, writing, opening, closing, and error handling with zstd-compressed files. It also stores the compression specification and initializes the private data pointer to NULL, which will later hold the ZstdCompressorState when compression operations begin.

## Parameters / Member Variables
- `CFH`: Pointer to the CompressFileHandle structure to be initialized with zstd functionality
- `compression_spec`: Structure containing compression-specific configuration parameters (level, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [CompressFileHandle](../C/CompressFileHandle.md) (structure)
  - [pg_compress_specification](../p/pg_compress_specification.md) (structure)  
  - [Zstd_open](../Z/Zstd_open.md) (function pointer assignment)
  - [Zstd_open_write](../Z/Zstd_open_write.md) (function pointer assignment)
  - [Zstd_read](../Z/Zstd_read.md) (function pointer assignment)
  - [Zstd_write](../Z/Zstd_write.md) (function pointer assignment)
  - [Zstd_gets](../Z/Zstd_gets.md) (function pointer assignment)
  - [Zstd_getc](../Z/Zstd_getc.md) (function pointer assignment)
  - [Zstd_close](../Z/Zstd_close.md) (function pointer assignment)
  - [Zstd_eof](../Z/Zstd_eof.md) (function pointer assignment)
  - [Zstd_get_error](../Z/Zstd_get_error.md) (function pointer assignment)
- Called from (representative examples):
  - [InitCompressFileHandle](InitCompressFileHandle.md) (main compression backend dispatcher)

## Notes and Other Information
- This is a public function (not static) that serves as the entry point for zstd compression
- Creates a complete function table for zstd operations through the generic compression interface
- The private_data field is initialized to NULL and allocated later during actual compression operations
- Part of the modular compression system that allows pg_dump to support multiple compression formats
- The compression_spec parameter allows configuration of zstd-specific settings like compression level
# tar_write_compressed_data

## Location
[src/bin/pg_basebackup/walmethods.c:713-764](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/walmethods.c#L713-L764)

## Overview
Compresses data using zlib compression and writes it to a TAR archive file during PostgreSQL WAL streaming or base backup operations.

## Definition

```c
static bool
tar_write_compressed_data(TarMethodData *tar_data, const void *buf, size_t count,
						  bool flush)
```
## Detailed Description
This function handles the compression of data using zlib deflate algorithm and writes the compressed output to a TAR archive file. It operates on the TarMethodData structure which maintains the compression state and file descriptor. The function processes input data through the zlib compression stream, handling output buffering and ensuring all compressed data is written to the underlying file. When the flush parameter is true, it forces completion of compression and resets the stream for subsequent writes.

## Parameters / Member Variables
- `tar_data`: Pointer to TarMethodData structure containing compression state, file descriptor, and output buffer
- `buf`: Pointer to input data buffer to be compressed
- `count`: Number of bytes in the input buffer to compress
- `flush`: Boolean flag indicating whether to flush and finalize the compression stream

## Dependencies
- Functions called/Symbols referenced:
  - deflate (zlib compression function)
  - deflateReset (zlib stream reset function)
  - write (system call for writing to file descriptor)
  - [TarMethodData](../T/TarMethodData.md) (data structure type)
  - ZLIB_OUT_SIZE (constant defining output buffer size)
- Called from:
  - [tar_write](tar_write.md)
  - [tar_open_for_write](tar_open_for_write.md)
  - [tar_close](tar_close.md)
  - [tar_finish](tar_finish.md)

## Notes and Other Information
- Returns false on compression errors, write failures, or stream reset failures
- Sets appropriate error codes in tar_data->base.lasterrno and error messages in tar_data->base.lasterrstring
- Handles partial writes by managing zlib output buffer state
- Assumes ENOSPC (no space left on device) when write() fails without setting errno
- The function is critical for compressed WAL archiving and base backup functionality
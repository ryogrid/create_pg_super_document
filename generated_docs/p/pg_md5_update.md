# pg_md5_update

## Location
src/common/md5.c: 400 - 431

## Overview
Processes input data incrementally for MD5 hash computation, buffering data and invoking the core MD5 algorithm on complete 64-byte blocks.

## Definition
```c
void pg_md5_update(pg_md5_ctx *ctx, const uint8 *data, size_t len)
```

## Detailed Description
The `pg_md5_update` function is the core data processing routine in PostgreSQL's MD5 implementation that handles arbitrary-length input data. It manages internal buffering to ensure that the MD5 algorithm processes data in the required 512-bit (64-byte) blocks. The function can be called multiple times to incrementally process data, making it suitable for hashing large files or streaming data.

The function maintains a running count of processed bits (md5_n) and uses an internal buffer (md5_buf) to accumulate data until complete 64-byte blocks are available. When sufficient data is present, it calls md5_calc() to process complete blocks. If the input spans multiple blocks, it processes them efficiently in a loop. Any remaining partial data is buffered for the next update call or final processing.

## Parameters / Member Variables
- `ctx`: Pointer to the MD5 context structure maintaining the hash state and internal buffer
- `data`: Pointer to the input data bytes to be processed
- `len`: Number of bytes to process from the data buffer

## Dependencies
- Functions called/Symbols referenced:
  - pg_md5_ctx (MD5 context structure type)
  - MD5_BUFLEN (constant defining MD5 buffer length - 64 bytes)
  - md5_calc (core MD5 block processing function)
  - memmove (standard library function for memory copying)
- Called from (representative examples):
  - pg_cryptohash_update

## Notes and Other Information
- This is a public function (non-static) and part of PostgreSQL's external MD5 API
- Can be called multiple times with arbitrary data lengths to build up the complete message
- Efficiently handles both small incremental updates and large data blocks
- Maintains bit count (md5_n) by multiplying byte count by 8, as MD5 specification requires bit-level length tracking
- Uses memmove() for safe memory copying that handles overlapping memory regions
- The internal buffer (md5_buf) size is exactly MD5_BUFLEN (64 bytes) to match MD5 block size requirements
- Critical middle step in PostgreSQL's MD5 workflow: init → update → final
- Thread-safe as it only modifies the provided context structure
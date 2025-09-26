# pglz_decompress

## Location
[src/common/pg_lzcompress.c:692-845](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_lzcompress.c#L692-L845)

## Overview
Decompresses PostgreSQL LZ-compressed data by interpreting control bytes and reconstructing the original data from literal bytes and back-reference match tags.

## Definition
```c
int32 pglz_decompress(const char *source, int32 slen, char *dest,
                      int32 rawsize, bool check_complete)
```

## Detailed Description
This function reverses the compression process by reading the compressed data format and reconstructing the original uncompressed data. It processes control bytes that indicate whether each subsequent data unit is a literal byte (copied directly) or a match tag (back-reference to previously decompressed data).

The decompression algorithm:
- Reads control bytes where each bit controls interpretation of next 8 data items
- For literal bytes (control bit = 0): copies byte directly from input to output
- For match tags (control bit = 1): decodes 2-3 byte tags containing offset and length
- Handles overlapping copy operations correctly for repeated pattern expansion
- Includes corruption detection for malformed compressed data

Match tag format uses 2 bytes for offset ≤ 4095 and length ≤ 17, with an optional third byte for longer matches. The function carefully handles overlapping memory regions during back-reference copying to properly expand repeated patterns.

## Parameters
- `source`: Compressed input data buffer
- `slen`: Length of compressed source data  
- `dest`: Output buffer for decompressed data
- `rawsize`: Expected size of decompressed data
- `check_complete`: If true, verifies exact input consumption and output generation

## Dependencies
- Functions called/Symbols referenced:
  - No external function dependencies (uses only standard C library functions)
- Called from:
  - [pglz_decompress_datum](pglz_decompress_datum.md) (TOAST decompression wrapper)
  - [pglz_decompress_datum_slice](pglz_decompress_datum_slice.md) (partial TOAST decompression)
  - [RestoreBlockImage](../R/RestoreBlockImage.md) (WAL record decompression)

## Notes and Other Information
- Returns number of bytes decompressed on success, -1 on corruption/error
- Implements careful overlapping memory copy logic for pattern expansion
- Control bit encoding: 0 = literal byte, 1 = match tag (back-reference)
- Match tag encoding: first byte contains length-3 in lower nibble, upper offset bits in upper nibble
- Includes comprehensive corruption detection including offset validation
- Essential component of PostgreSQL's TOAST decompression and WAL recovery systems
- Optimized for both safety (corruption detection) and performance (efficient copying)
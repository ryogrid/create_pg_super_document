# LZ4Stream_eof

## Location
[src/bin/pg_dump/compress_lz4.c:322-329](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_lz4.c#L322-L329)

## Overview
LZ4 equivalent to the standard feof() function, determining if an LZ4-compressed stream has reached end-of-file by checking both the overflow buffer and the underlying file stream.

## Definition
```c
static bool LZ4Stream_eof(CompressFileHandle *CFH)
```

## Detailed Description
This function implements end-of-file detection for LZ4-compressed streams in PostgreSQL's Stream API. It provides functionality equivalent to the standard library's feof() or zlib's gzeof() functions, but for LZ4-compressed data. The function returns true only when both conditions are met: there is no buffered decompressed data remaining in the overflow buffer (overflowlen == 0) and the underlying file stream has reached its end (feof(state->fp) returns true). This dual check ensures that all available data has been consumed before reporting end-of-file status.

## Parameters / Member Variables
- `CFH`: Pointer to the CompressFileHandle structure representing the compressed file stream

## Dependencies
- Functions called/Symbols referenced:
  - feof (standard library function)
- Types used:
  - [CompressFileHandle](../C/CompressFileHandle.md)
  - [LZ4State](LZ4State.md)
- Called from (representative examples):
  - [LZ4Stream_getc](LZ4Stream_getc.md)

## Notes and Other Information
- This is a static function internal to the compress_lz4.c module
- Part of PostgreSQL's LZ4 Stream API implementation
- Returns true only when both overflow buffer is empty AND underlying file stream is at EOF
- Essential for proper stream processing to ensure all buffered data is consumed
- Used by higher-level stream functions like LZ4Stream_getc for proper EOF handling
- The dual condition check prevents premature EOF detection when data remains in buffers
- Analogous to standard library EOF detection but accounting for decompression buffering
- Critical for stream-based reading operations that need to know when all data is exhausted

## Simplified Source

```c
static bool
LZ4Stream_eof(CompressFileHandle *CFH)
{
    LZ4State *state = (LZ4State *) CFH->private_data;

    // EOF only when no buffered data AND underlying file at EOF
    return state->overflowlen == 0 && feof(state->fp);
}
```
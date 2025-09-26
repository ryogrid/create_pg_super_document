# pglz_compress

## Location
[src/common/pg_lzcompress.c:509-691](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_lzcompress.c#L509-L691)

## Overview
Main compression function that implements PostgreSQL's LZ compression algorithm, transforming raw input data into compressed format using history-based pattern matching and adaptive strategies.

## Definition
```c
int32 pglz_compress(const char *source, int32 slen, char *dest,
                    const PGLZ_Strategy *strategy)
```

## Detailed Description
This function compresses input data using a sliding window LZ compression algorithm with configurable compression strategies. It maintains a hash-based history table to quickly locate repeated sequences and uses control bytes to encode whether each group of 8 data units represents literal bytes or back-references to previously seen data.

The compression process involves:
- Initializing hash table for pattern matching based on input size
- Iteratively processing input while searching for matches in history
- Encoding matches as 2-3 byte tags (offset + length) or copying literals
- Maintaining compression statistics to abort early on incompressible data
- Enforcing maximum output size limits based on strategy requirements

The algorithm uses adaptive hash table sizing (512 to 8192 entries) and implements early termination for incompressible data to optimize performance.

## Parameters
- `source`: Input data buffer to compress
- `slen`: Length of source data in bytes
- `dest`: Output buffer for compressed data
- `strategy`: Compression strategy parameters (NULL uses default strategy)

## Dependencies
- Functions called/Symbols referenced:
  - PGLZ_Strategy (strategy configuration structure)
  - PGLZ_MAX_MATCH (maximum match length constant)
  - pglz_find_match (pattern matching function)
  - pglz_out_tag (macro to output match tags)
  - pglz_out_literal (macro to output literal bytes)
  - pglz_hist_add (macro to add entries to history table)
- Called from:
  - pglz_compress_datum (TOAST compression wrapper)
  - XLogCompressBackupBlock (WAL compression)

## Notes and Other Information
- Returns compressed size on success, -1 on failure/incompressible data
- Uses control bytes where each bit indicates literal (0) or match (1) for next 8 items
- Hash table size automatically selected based on input size for optimal performance
- Implements early failure detection for pre-compressed or random data
- Strategy parameters control match quality, minimum compression ratios, and size limits
- Critical component of PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) system
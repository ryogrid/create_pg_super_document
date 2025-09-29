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
  - [PGLZ_Strategy](../P/PGLZ_Strategy.md) (strategy configuration structure)
  - PGLZ_MAX_MATCH (maximum match length constant)
  - [pglz_find_match](pglz_find_match.md) (pattern matching function)
  - pglz_out_tag (macro to output match tags)
  - pglz_out_literal (macro to output literal bytes)
  - pglz_hist_add (macro to add entries to history table)
- Called from:
  - [pglz_compress_datum](pglz_compress_datum.md) (TOAST compression wrapper)
  - [XLogCompressBackupBlock](../X/XLogCompressBackupBlock.md) (WAL compression)

## Notes and Other Information
- Returns compressed size on success, -1 on failure/incompressible data
- Uses control bytes where each bit indicates literal (0) or match (1) for next 8 items
- [Hash](../H/Hash.md) table size automatically selected based on input size for optimal performance
- Implements early failure detection for pre-compressed or random data
- Strategy parameters control match quality, minimum compression ratios, and size limits
- Critical component of PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) system

## Simplified Source

```c
int32 pglz_compress(const char *source, int32 slen, char *dest,
                    const PGLZ_Strategy *strategy)
{
    // Setup variables for compression tracking
    unsigned char *bp = (unsigned char *) dest;
    unsigned char *bstart = bp;
    const char *dp = source;
    const char *dend = source + slen;
    bool found_match = false;
    int32 match_len, match_off;
    int32 good_match, good_drop, need_rate;
    int32 result_max;
    int hashsz, mask;

    // Use default strategy if none provided
    if (strategy == NULL)
        strategy = PGLZ_strategy_default;

    // Validate compression strategy parameters
    if (strategy->match_size_good <= 0 ||
        slen < strategy->min_input_size ||
        slen > strategy->max_input_size)
        return -1;

    // Configure match parameters within supported ranges
    good_match = strategy->match_size_good;
    if (good_match > PGLZ_MAX_MATCH) good_match = PGLZ_MAX_MATCH;
    else if (good_match < 17) good_match = 17;

    good_drop = strategy->match_size_drop;
    if (good_drop < 0) good_drop = 0;
    else if (good_drop > 100) good_drop = 100;

    need_rate = strategy->min_comp_rate;
    if (need_rate < 0) need_rate = 0;
    else if (need_rate > 99) need_rate = 99;

    // Calculate maximum allowed output size
    if (slen > (INT_MAX / 100))
        result_max = (slen / 100) * (100 - need_rate);
    else
        result_max = (slen * (100 - need_rate)) / 100;

    // Choose hash table size based on input size
    if (slen < 128) hashsz = 512;
    else if (slen < 256) hashsz = 1024;
    else if (slen < 512) hashsz = 2048;
    else if (slen < 1024) hashsz = 4096;
    else hashsz = 8192;
    mask = hashsz - 1;

    // Initialize history table
    memset(hist_start, 0, hashsz * sizeof(int16));

    // Main compression loop
    while (dp < dend) {
        // Check if output size limit exceeded
        if (bp - bstart >= result_max)
            return -1;

        // Early failure for incompressible data
        if (!found_match && bp - bstart >= strategy->first_success_by)
            return -1;

        // Try to find a match in history
        if (pglz_find_match(hist_start, dp, dend, &match_len,
                           &match_off, good_match, good_drop, mask)) {
            // Output match tag and add history entries
            pglz_out_tag(ctrlp, ctrlb, ctrl, bp, match_len, match_off);
            while (match_len--) {
                pglz_hist_add(hist_start, hist_entries,
                             hist_next, hist_recycle, dp, dend, mask);
                dp++;
            }
            found_match = true;
        } else {
            // No match found - output literal byte
            pglz_out_literal(ctrlp, ctrlb, ctrl, bp, *dp);
            pglz_hist_add(hist_start, hist_entries,
                         hist_next, hist_recycle, dp, dend, mask);
            dp++;
        }
    }

    // Finalize compression and validate size
    *ctrlp = ctrlb;
    int32 result_size = bp - bstart;
    if (result_size >= result_max)
        return -1;

    return result_size;  // Success
}
```
# pglz_maximum_compressed_size

## Location
src/common/pg_lzcompress.c: 846 - 876

## Overview
Calculates the theoretical maximum compressed size for a given amount of raw data, accounting for worst-case compression scenarios and encoding overhead.

## Definition
```c
int32 pglz_maximum_compressed_size(int32 rawsize, int32 total_compressed_size)
```

## Detailed Description
This function computes the maximum number of bytes that might be needed to represent a prefix of specified raw data size within compressed data. It accounts for the worst-case scenario where all data is represented as literal bytes rather than compressed matches.

The calculation considers:
- Control bit overhead: 1 control bit per 8 data bytes, requiring 9 bits total per input byte in worst case
- Rounding overhead for bit-to-byte conversion
- Corner case buffer where final match tags might require additional bytes
- Ensures result doesn't exceed total compressed data size

This function is essential for safe partial decompression operations where only a prefix of the original data is needed, allowing proper buffer allocation without over-reading compressed data.

## Parameters
- `rawsize`: Number of raw (uncompressed) bytes for which to calculate maximum compressed size
- `total_compressed_size`: Total size of the complete compressed data buffer

## Dependencies
- Functions called/Symbols referenced:
  - Min (macro for minimum value selection)
- Called from:
  - detoast_attr_slice (TOAST partial decompression)

## Notes and Other Information
- Returns minimum of calculated maximum size and total compressed size
- Accounts for 1 control bit per data byte plus control byte overhead
- Adds 2 extra bytes to handle corner case of partial match tags at end
- Uses 64-bit arithmetic internally to prevent overflow during calculation
- Critical for safe partial decompression in TOAST system
- Prevents buffer overruns when extracting slices from compressed TOAST data
- Formula: ((rawsize * 9 + 7) / 8) + 2, capped at total_compressed_size
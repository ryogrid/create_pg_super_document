# PGLZ_Strategy

## Location
src/include/common/pg_lzcompress.h: 57 - 65

## Overview
PGLZ_Strategy is a configuration structure that controls the behavior and parameters of PostgreSQL's built-in LZ compression algorithm, allowing fine-tuning of compression performance versus efficiency trade-offs.

## Definition

```c
typedef struct PGLZ_Strategy
{
	int32		min_input_size;
	int32		max_input_size;
	int32		min_comp_rate;
	int32		first_success_by;
	int32		match_size_good;
	int32		match_size_drop;
} PGLZ_Strategy;
```
## Detailed Description
The PGLZ_Strategy structure provides configuration parameters that control the compression algorithm's behavior in the PostgreSQL LZ compression implementation. It allows the system to balance between compression ratio, speed, and resource usage by setting various thresholds and limits.

The strategy is used by the  function to determine when and how to compress data. Different strategies can be employed for different use cases - for example, TOAST data might use a more aggressive compression strategy than temporary data that needs fast access.

The compression algorithm uses a history-based approach with configurable parameters for match finding and early termination conditions. The strategy allows tuning these parameters based on the expected data characteristics and performance requirements.

## Parameters / Member Variables
- : Minimum input data size (in bytes) to consider for compression. Data smaller than this threshold will not be compressed.
- : Maximum input data size (in bytes) to consider for compression. Data larger than this threshold will not be compressed.  
- : Minimum compression rate (0-99%) required for the compression to be considered successful. If compression doesn't achieve this rate, the original uncompressed data is used instead.
- : Early termination threshold - abandon compression if no compressible data is found within the first this-many bytes of input.
- : Initial "good" match size when starting history lookup. This value determines what constitutes a satisfactory match during the compression process and is lowered iteratively to allow smaller matches as the search continues.
- : Percentage (0-100) by which  is reduced after each history check iteration. A value of 0 means no change until the end, while 100 means only the latest history entry is checked.

## Dependencies
- Functions called/Symbols referenced:
  - (No direct function calls - this is a data structure)

- Used by:
  -  (primary consumer at src/common/pg_lzcompress.c:510)

## Notes and Other Information
- Two standard strategy instances are provided:
  - : Recommended for TOAST compression with conservative settings (min_input_size=32, min_comp_rate=25%, first_success_by=1024)
  - : Attempts compression on inputs of any length with minimal compression requirements

- The compression algorithm has built-in limits that override strategy settings if they exceed supported ranges:
  -  is clamped between 17 and PGLZ_MAX_MATCH
  -  is clamped between 0 and 100
  -  is clamped between 0 and 99

- If NULL is passed as the strategy parameter to , the default strategy is automatically used

- The strategy system allows PostgreSQL to adapt compression behavior for different contexts, such as TOAST storage versus temporary data compression, balancing compression ratio against CPU overhead
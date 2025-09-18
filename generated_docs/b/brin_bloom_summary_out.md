# brin_bloom_summary_out

## Location
src/backend/access/brin/brin_bloom.c: 799 - 822

## Overview
Output function for the brin_bloom_summary PostgreSQL data type that converts internal binary bloom filter data into a human-readable text representation showing the filter's key characteristics.

## Definition
```c
Datum brin_bloom_summary_out(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the output routine for the brin_bloom_summary data type, converting the internal binary bloom filter representation into a readable text format. It deserializes the bloom filter data and extracts key metrics including the number of hash functions, total bits in the bitmap, and the number of bits currently set. The output format provides essential debugging and monitoring information about the bloom filter's state, formatted as a structured string with labeled fields.

## Parameters / Member Variables
- No direct parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)
- Operates on a brin_bloom_summary datum from the argument list

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM (to handle potentially compressed data)
  - PG_GETARG_DATUM 
  - initStringInfo
  - appendStringInfoChar
  - appendStringInfo
  - PG_RETURN_CSTRING
- Data types used:
  - BloomFilter
  - StringInfoData
- Called from (representative examples):
  - PostgreSQL type system (when converting to text representation)

## Notes and Other Information
- Output format: "{mode: hashed  nhashes: X  nbits: Y  nbits_set: Z}"
  - nhashes: Number of hash functions used by the bloom filter
  - nbits: Total number of bits in the bloom filter bitmap
  - nbits_set: Number of bits currently set to 1 (indicates filter fullness)
- The function handles potentially compressed (TOAST-ed) input data via PG_DETOAST_DATUM
- Mode is always displayed as "hashed" indicating the bloom filter implementation type
- This output is primarily useful for debugging and monitoring bloom filter effectiveness
- The ratio of nbits_set to nbits can indicate the filter's false positive rate characteristics
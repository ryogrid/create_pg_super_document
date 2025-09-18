# BloomOptions

## Location
src/backend/access/brin/brin_bloom.c: 154 - 159

## Overview
BloomOptions is a structure that stores configuration options for BRIN's bloom filter indexes, specifically controlling the distinct value estimation and false positive rate parameters.

## Definition


## Detailed Description
BloomOptions serves as the storage type for BRIN's reloptions (relation options) specific to bloom filter indexes. This structure encapsulates the two key parameters that control bloom filter behavior: the expected number of distinct values per range and the target false positive rate. These parameters are critical for optimizing bloom filter size and performance characteristics. The structure follows PostgreSQL's varlena format, making it suitable for storage as relation options.

## Parameters / Member Variables
- : Standard varlena header required for PostgreSQL's variable-length data types (should not be manipulated directly)
- : Controls the expected number of distinct values within each BRIN range, used to calculate optimal bloom filter size
- : Target false positive rate for the bloom filter, balancing between filter size and accuracy

## Dependencies
- Functions called/Symbols referenced: None directly
- Called from (representative examples):
  - BloomGetNDistinctPerRange
  - BloomGetFalsePositiveRate
  - brin_bloom_get_ndistinct
  - brin_bloom_add_value
  - brin_bloom_options

## Notes and Other Information
This structure is part of PostgreSQL's BRIN (Block Range Index) bloom filter implementation, located in src/backend/access/brin/brin_bloom.c:154-159. The bloom filter parameters stored here directly impact memory usage and query performance, making proper configuration essential for optimal index behavior.
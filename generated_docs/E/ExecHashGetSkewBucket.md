# ExecHashGetSkewBucket

## Location
src/backend/executor/nodeHash.c: 2535 - 2580

## Overview
Retrieves the index of the skew bucket associated with a given hash value, or returns INVALID_SKEW_BUCKET_NO if no active skew bucket matches.

## Definition


## Detailed Description
ExecHashGetSkewBucket performs a lookup in the skew hash table to find the bucket index corresponding to a specific hash value. This function is a key component of PostgreSQL's skew optimization for hash joins, allowing the executor to quickly determine if a hash value corresponds to one of the most common values (MCVs) that have dedicated skew buckets.

The function uses open addressing with linear probing to handle hash collisions. It starts by computing the initial bucket position using a bitwise AND operation (taking advantage of the power-of-2 sizing), then probes linearly until it either finds the matching bucket or encounters a NULL slot indicating the hash value is not present.

The function returns INVALID_SKEW_BUCKET_NO in two cases: when skew optimization is disabled (typically after the initial batch), or when no skew bucket exists for the given hash value.

## Parameters / Member Variables
- : The HashJoinTable containing the skew bucket array to search
- : The 32-bit hash value to look up in the skew bucket table

## Dependencies
- Functions called/Symbols referenced:
  - INVALID_SKEW_BUCKET_NO (constant returned when no bucket found)
- Called from:
  - MultiExecPrivateHash (during hash table population)
  - ExecHashJoinImpl (during hash join probing)
  - Referenced in nodeHash.h header

## Notes and Other Information
- Always returns INVALID_SKEW_BUCKET_NO when skew optimization is disabled
- Uses efficient bitwise AND for modulo operation due to power-of-2 bucket array sizing
- Implements linear probing for collision resolution, matching the logic in ExecHashBuildSkewHash
- Critical for performance during hash join probe phase to quickly identify MCV lookups
- Skew optimization is typically disabled after the initial batch is processed
- The linear probing must eventually terminate due to guaranteed NULL slots in the bucket array
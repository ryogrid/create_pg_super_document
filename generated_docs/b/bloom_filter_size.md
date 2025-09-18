# bloom_filter_size

## Location
src/backend/access/brin/brin_bloom.c: 271 - 309

## Overview
Calculates optimal Bloom filter parameters (number of bits, bytes, and hash functions) given the expected number of distinct values and desired false positive rate.

## Definition


## Detailed Description
This function implements the mathematical formulas for calculating optimal Bloom filter parameters. It uses the standard Bloom filter sizing formula: -(n * ln(p)) / (ln(2))^2 to determine the optimal number of bits, then rounds up to whole bytes. The optimal number of hash functions is calculated using the formula: round(log(2.0) * m / ndistinct), where m is the number of bits and ndistinct is the expected number of distinct values.

The function ensures the filter size is rounded to whole bytes and implements a portable rounding mechanism that doesn't rely on the round() function, which may not be available on all platforms (particularly Windows).

## Parameters / Member Variables
- : Expected number of distinct values to be stored in the Bloom filter
- : Desired false positive rate (probability between 0 and 1)
- : Output pointer to store the calculated number of bytes (can be NULL if not needed)
- : Output pointer to store the calculated number of bits (can be NULL if not needed)  
- : Output pointer to store the calculated number of hash functions (can be NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - BloomFilter (data structure)
  - Mathematical functions: ceil, log, pow, floor
- Called from (representative examples):
  - bloom_init

## Notes and Other Information
- The function is static, meaning it's only accessible within the brin_bloom.c file
- Implements portable rounding logic to avoid dependency on round() function
- The sizing formula is based on standard Bloom filter theory for optimal space/accuracy trade-offs
- All output parameters are optional (can be NULL) allowing caller to retrieve only needed values
- Located in src/backend/access/brin/brin_bloom.c:271-309
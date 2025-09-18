# hemdistsign

## Location
src/backend/utils/adt/tsgistidx.c: 496 - 511

## Overview
The hemdistsign function calculates the Hamming distance between two bit vector signatures, measuring the number of differing bits for TSVector GiST index operations.

## Definition


## Detailed Description
This function computes the Hamming distance between two bit vector signatures by performing an XOR operation on each byte and counting the number of set bits in the result. It uses a byte-wise approach with a lookup table (pg_number_of_ones) to efficiently count bits rather than using popcount functions, as noted in the comment that popcount isn't likely to provide performance benefits for this specific use case. The Hamming distance is crucial for determining similarity between signatures in the GiST index structure.

## Parameters / Member Variables
- : First bit vector signature for comparison
- : Second bit vector signature for comparison  
- : Length of the signatures in bytes
- Internal variables:
  - : Loop counter for byte iteration
  - : XOR result of current byte pair
  - : Accumulator for total distance (number of differing bits)

## Dependencies
- Functions called/Symbols referenced:
  - LOOPBYTE (macro for byte-wise iteration)
  - pg_number_of_ones (lookup table for counting bits in a byte)
  - BITVECP (typedef for bit vector pointer)
- Called from (representative examples):
  - hemdist (higher-level Hamming distance function)
  - gtsvector_penalty (for penalty calculations in index operations)
  - hemdistcache (for cached distance calculations)
  - gtsvector_picksplit (for page splitting decisions)

## Notes and Other Information
This is a static helper function optimized for byte-wise Hamming distance calculation. The implementation deliberately uses a lookup table approach rather than popcount functions, as indicated by the inline comment. This choice reflects performance considerations specific to the typical signature sizes and usage patterns in TSVector GiST indexes. The function is fundamental to many GiST operations that need to measure signature similarity.
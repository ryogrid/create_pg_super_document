# hemdistcache

## Location
src/backend/utils/adt/tsgistidx.c: 605 - 620

## Overview
Calculates the Hamming distance between two cached signature structures, providing optimized distance computation for GiST index operations that work with pre-processed signature data.

## Definition
```c
static int hemdistcache(CACHESIGN *a, CACHESIGN *b, int siglen)
```

## Detailed Description
The `hemdistcache` function computes the Hamming distance between two `CACHESIGN` structures, which contain cached signature information. This function is similar to `hemdist` but operates on cached signatures that have been preprocessed by `fillcache`, making it more efficient for repeated distance calculations during complex GiST operations.

The function handles three cases based on the `allistrue` flags:
1. **Both signatures are ALLTRUE**: Returns 0 (identical)
2. **One signature is ALLTRUE**: Returns the number of zero bits in the non-ALLTRUE signature
3. **Neither is ALLTRUE**: Delegates to `hemdistsign` for bit-by-bit comparison

This caching approach avoids repeated signature extraction and conversion during operations like node splitting where the same signatures are compared multiple times.

## Parameters / Member Variables
- `a`: First cached signature structure containing signature data and ALLTRUE flag
- `b`: Second cached signature structure containing signature data and ALLTRUE flag  
- `siglen`: Length of the signature in bytes

## Dependencies
- Functions called/Symbols referenced:
  - SIGLENBIT (macro to get bit length of signature)
  - sizebitvec (function to count set bits in signature)
  - hemdistsign (function to compute Hamming distance between bit vectors)
- Called from:
  - gtsvector_picksplit (multiple times during index node splitting operations)

## Notes and Other Information
- This is a static helper function used only within tsgistidx.c
- Works with CACHESIGN structures that contain both an `allistrue` boolean flag and a `sign` bit vector
- Provides performance optimization for operations that need repeated distance calculations
- Used extensively in GiST index splitting algorithms to determine optimal partitioning
- The caching mechanism eliminates redundant signature processing during complex index operations
- Functionally equivalent to `hemdist` but operates on preprocessed cache structures
- Located in src/backend/utils/adt/tsgistidx.c:605-620
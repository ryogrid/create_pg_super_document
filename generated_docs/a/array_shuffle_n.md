# array_shuffle_n

## Location
src/backend/utils/adt/array_userfuncs.c: 1537 - 1625

## Overview
A static helper function that returns a copy of an array containing n randomly chosen items from the first dimension, using the Fisher-Yates shuffle algorithm.

## Definition


## Detailed Description
 implements a partial Fisher-Yates shuffle algorithm to randomly select n items from the first dimension of a multi-dimensional PostgreSQL array. The function preserves the structure of lower-order dimensions while shuffling only along the first dimension. It performs in-place shuffling by swapping elements and stops after n iterations, making the first n items the randomly selected result.

The function handles multi-dimensional arrays by treating each "item" in the first dimension as consisting of  elements (where  is the product of all lower dimensions). During shuffling, entire items are swapped as units, preserving the internal structure of each item.

The algorithm uses PostgreSQL's global pseudo-random number generator () for randomization. Memory management includes proper cleanup of deconstructed array elements after constructing the result.

## Parameters / Member Variables
- : Input PostgreSQL array to shuffle
- : Number of items to select from the first dimension
- : Whether to preserve the original lower bound of the first dimension
- : OID of the element type
- : Type cache entry containing type information (typlen, typbyval, typalign)

## Dependencies
- Functions called/Symbols referenced:
  - ARR_NDIM, ARR_DIMS, ARR_LBOUND
  - construct_empty_array
  - deconstruct_array
  - pg_prng_uint64_range
  - construct_md_array
  - pfree
- Called from (representative examples):
  - array_shuffle
  - array_sample

## Notes and Other Information
- Static helper function not directly exposed to SQL
- Uses Fisher-Yates shuffle algorithm for uniform random selection
- Preserves lower-order dimensions while shuffling only the first dimension  
- Returns empty array for invalid inputs (empty array, n ≤ 0, etc.)
- Caller must ensure n ≤ size of first dimension (checked with Assert)
- Uses global PRNG state for randomization
- Memory efficient: only allocates result array, reuses input array elements
- Lower bound handling: preserves original if keep_lb=true, else sets to 1
- Supports both fixed-length and variable-length element types through TypeCacheEntry
- Located in src/backend/utils/adt/array_userfuncs.c:1537-1625
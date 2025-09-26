# ts_accum

## Location
src/backend/utils/adt/tsvector_op.c: 2413 - 2447

## Overview
An accumulator function that builds statistical information from TSVector data, serving as the core engine for PostgreSQL's ts_stat functionality by maintaining a binary search tree of word statistics.

## Definition


## Detailed Description
The  function is the central accumulator for PostgreSQL's text search statistics system. It processes TSVector data incrementally, building and maintaining statistical information about words across multiple documents. The function was originally designed to work as a custom aggregate function, but PostgreSQL's limitation on aggregates returning sets led to its current implementation as a helper function.

The function initializes a new TSVectorStat structure on the first call, then for each subsequent TSVector processes the words using a strategic sampling approach. It calculates optimal parameters for the sampling algorithm, including bit alignment and offset calculations to ensure even distribution of sampled words.

The core processing involves two key operations: first, it inserts a strategically chosen central word entry, then it delegates to  to recursively sample additional representative words from the TSVector. This approach balances statistical accuracy with performance, making it practical to generate statistics from large text corpora.

## Parameters / Member Variables
- : Memory context that ensures allocated TSVectorStat structures and tree nodes persist across function calls
- : Existing TSVectorStat structure to accumulate into, or NULL for first call to trigger initialization  
- : Datum containing the TSVector data to be processed and accumulated into statistics

## Dependencies
- Functions called/Symbols referenced:
  - : Converts Datum to TSVector for processing
  - : Allocates zero-initialized memory for new TSVectorStat structure
  - : Processes the central word entry from the TSVector
  - : Initiates recursive sampling of additional words
  - : Frees temporary TSVector if it was created during conversion
  - : Used for pointer comparison during cleanup
  - : Structure type for maintaining word statistics tree
  - : Text search vector type containing word and position data
- Called from (representative examples):
  - : Main SQL-callable function that uses ts_accum to build statistics

## Notes and Other Information
- Originally designed as an aggregate function but converted due to PostgreSQL limitations on aggregates returning sets
- The function includes detailed comments explaining the original intended usage pattern
- Implements sophisticated bit manipulation for optimal sampling parameter calculation
- Handles edge cases including NULL or empty TSVectors gracefully
- Manages memory carefully, freeing temporary TSVectors when appropriate
- The sampling strategy uses power-of-2 alignment for efficient recursive processing
- Part of PostgreSQL's full-text search statistics infrastructure
- The accumulator pattern allows processing large numbers of documents incrementally
- Located in 
- Critical for enabling statistical analysis of large text search corpora without excessive memory or CPU usage
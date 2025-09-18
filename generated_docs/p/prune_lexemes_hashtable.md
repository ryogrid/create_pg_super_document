# prune_lexemes_hashtable

## Location
src/backend/tsearch/ts_typanalyze.c: 453 - 477

## Overview
Prunes low-frequency entries from the lexemes hash table as part of the Lossy Counting algorithm implementation in tsvector statistics collection.

## Definition


## Detailed Description
This function implements the pruning phase of the Lossy Counting algorithm used in tsvector statistics collection. It removes entries from the hash table where the condition (frequency + delta <= b_current) is satisfied, which eliminates lexemes that are unlikely to meet the final frequency threshold. The pruning helps keep memory usage bounded while maintaining algorithm accuracy guarantees.

## Parameters / Member Variables
- : Hash table containing TrackItem entries with lexeme frequencies and deltas
- : Current batch number in the Lossy Counting algorithm

## Dependencies
- Functions called/Symbols referenced:
  - HTAB
  - HASH_SEQ_STATUS
  - TrackItem
  - hash_seq_init
  - hash_seq_search
  - hash_search
  - HASH_REMOVE
- Called from (representative examples):
  - compute_tsvector_stats

## Notes and Other Information
- Part of the Lossy Counting algorithm's D structure maintenance
- Removes entries where frequency + delta <= b_current (algorithm pruning condition)
- Frees memory for removed lexeme strings using pfree()
- Error handling for hash table corruption detection
- Called after processing each bucket_width worth of lexemes
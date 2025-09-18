# compute_tsvector_stats

## Location
src/backend/tsearch/ts_typanalyze.c: 141 - 452

## Overview
Computes statistics for tsvector columns using the Lossy Counting algorithm to identify the most common lexemes and their frequencies for query selectivity estimation.

## Definition


## Detailed Description
This function implements statistics collection for tsvector columns by finding the most common lexemes rather than most common values (since tsvectors are typically unique). It uses the Lossy Counting algorithm from Manku and Motwani to efficiently track lexeme frequencies in a streaming fashion. The algorithm maintains a hash table of lexemes with their frequencies and periodically prunes low-frequency entries. The resulting statistics are stored in the MCELEM slot of pg_statistic to support @@ operator selectivity estimation.

## Parameters / Member Variables
- : VacAttrStats structure to populate with computed statistics
- : Function to fetch sample values from the column
- : Number of rows in the sample
- : Total number of rows in the table (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - VacAttrStats
  - HTAB
  - HASHCTL
  - LexemeHashKey
  - TrackItem
  - lexeme_hash
  - lexeme_match
  - prune_lexemes_hashtable
  - hash_create
  - TSVector
  - WordEntry
  - vacuum_delay_point
  - VARSIZE_ANY
  - DatumGetTSVector
  - STRPTR
  - ARRPTR
  - hash_search
  - hash_get_num_entries
  - hash_seq_init
  - hash_seq_search
  - trackitem_compare_frequencies_desc
  - trackitem_compare_lexemes
  - cstring_to_text_with_len
- Called from (representative examples):
  - ts_typanalyze (via function pointer assignment)

## Notes and Other Information
- Uses Lossy Counting algorithm with bucket width = (num_mcelem + 10) * 1000 / 7
- Assumes tsvector columns are unique (stadistinct = -1.0)
- Target is statistics_target * 10 lexemes in MCELEM array
- Stores lexemes sorted by length then lexicographically for binary search efficiency
- Includes min/max frequencies in extra mcelem_freqs slots
- Frequency calculations are relative to non-null row count, not total lexeme count
- Based on Zipfian distribution assumptions for natural language lexeme frequencies
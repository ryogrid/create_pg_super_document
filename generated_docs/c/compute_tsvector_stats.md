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
  - [HTAB](../H/HTAB.md)
  - HASHCTL
  - LexemeHashKey
  - [TrackItem](../T/TrackItem.md)
  - [lexeme_hash](../l/lexeme_hash.md)
  - [lexeme_match](../l/lexeme_match.md)
  - [prune_lexemes_hashtable](../p/prune_lexemes_hashtable.md)
  - [hash_create](../h/hash_create.md)
  - TSVector
  - WordEntry
  - [vacuum_delay_point](../v/vacuum_delay_point.md)
  - VARSIZE_ANY
  - [DatumGetTSVector](../D/DatumGetTSVector.md)
  - STRPTR
  - ARRPTR
  - [hash_search](../h/hash_search.md)
  - [hash_get_num_entries](../h/hash_get_num_entries.md)
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [trackitem_compare_frequencies_desc](../t/trackitem_compare_frequencies_desc.md)
  - [trackitem_compare_lexemes](../t/trackitem_compare_lexemes.md)
  - cstring_to_text_with_len
- Called from (representative examples):
  - [ts_typanalyze](../t/ts_typanalyze.md) (via function pointer assignment)

## Notes and Other Information
- Uses Lossy Counting algorithm with bucket width = (num_mcelem + 10) * 1000 / 7
- Assumes tsvector columns are unique (stadistinct = -1.0)
- Target is statistics_target * 10 lexemes in MCELEM array
- Stores lexemes sorted by length then lexicographically for binary search efficiency
- Includes min/max frequencies in extra mcelem_freqs slots
- Frequency calculations are relative to non-null row count, not total lexeme count
- Based on Zipfian distribution assumptions for natural language lexeme frequencies
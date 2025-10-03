# prune_lexemes_hashtable

## Location
[src/backend/tsearch/ts_typanalyze.c:453-477](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_typanalyze.c#L453-L477)

## Overview
Prunes low-frequency entries from the lexemes hash table as part of the Lossy Counting algorithm implementation in tsvector statistics collection.

## Definition

```c
static void
prune_lexemes_hashtable(HTAB *lexemes_tab, int b_current)
```
## Detailed Description
This function implements the pruning phase of the Lossy Counting algorithm used in tsvector statistics collection. It removes entries from the hash table where the condition (frequency + delta <= b_current) is satisfied, which eliminates lexemes that are unlikely to meet the final frequency threshold. The pruning helps keep memory usage bounded while maintaining algorithm accuracy guarantees.

## Parameters / Member Variables
- `*lexemes_tab`: Hash table containing TrackItem entries with lexeme frequencies and deltas
- `b_current`: Current batch number in the Lossy Counting algorithm
## Dependencies
- Functions called/Symbols referenced:
  - [HTAB](../H/HTAB.md)
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md)
  - [TrackItem](../T/TrackItem.md)
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [hash_search](../h/hash_search.md)
  - HASH_REMOVE
- Called from (representative examples):
  - [compute_tsvector_stats](../c/compute_tsvector_stats.md)

## Notes and Other Information
- Part of the Lossy Counting algorithm's D structure maintenance
- Removes entries where frequency + delta <= b_current (algorithm pruning condition)
- Frees memory for removed lexeme strings using pfree()
- Error handling for hash table corruption detection
- Called after processing each bucket_width worth of lexemes
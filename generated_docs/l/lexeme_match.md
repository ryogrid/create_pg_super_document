# lexeme_match

## Location
src/backend/tsearch/ts_typanalyze.c: 490 - 499

## Overview
A matching function for lexemes used in hash table lookups that delegates to lexeme_compare for actual comparison logic.

## Definition


## Detailed Description
This function serves as a wrapper around lexeme_compare for use in PostgreSQL hash table operations. It provides the standard hash table matching function interface while delegating the actual comparison logic to lexeme_compare. The function is designed to work with LexemeHashKey structures that contain non-null-terminated lexeme strings with explicit lengths.

## Parameters / Member Variables
- : First LexemeHashKey to compare
- : Second LexemeHashKey to compare  
- : Size parameter (unused, as keys contain their own length information)

## Dependencies
- Functions called/Symbols referenced:
  - [lexeme_compare](lexeme_compare.md)
- Called from (representative examples):
  - [compute_tsvector_stats](../c/compute_tsvector_stats.md) (via hash table configuration)

## Notes and Other Information
- The keysize parameter is superfluous since LexemeHashKey structures store their own lengths
- Part of hash table setup for Lossy Counting algorithm in tsvector statistics
- Returns 0 for equal keys, non-zero for different keys (following lexeme_compare semantics)
- [lexeme_compare](lexeme_compare.md) first compares by length, then byte-by-byte using strncmp()
- Used in conjunction with lexeme_hash for complete hash table key operations
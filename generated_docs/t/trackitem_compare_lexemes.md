# trackitem_compare_lexemes

## Location
[src/backend/tsearch/ts_typanalyze.c:530-536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_typanalyze.c#L530-L536)

## Overview
A static comparator function for sorting TrackItem arrays by their lexeme values using lexeme comparison logic.

## Definition
static int trackitem_compare_lexemes(const void *e1, const void *e2, void *arg)

## Detailed Description
This function serves as a wrapper comparator that sorts TrackItem structures based on their lexeme keys. It extracts the LexemeHashKey from each TrackItem and delegates the actual comparison to the lexeme_compare function. This enables sorting of track items lexicographically by their lexeme content, which is useful for organizing text search statistics data.

## Parameters / Member Variables
- e1: Pointer to the first TrackItem pointer to compare
- e2: Pointer to the second TrackItem pointer to compare
- arg: Additional argument for the comparator (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [TrackItem](../T/TrackItem.md) (struct type)
  - [lexeme_compare](../l/lexeme_compare.md) (comparison function)
  - LexemeHashKey (accessed via TrackItem key field)
- Called from (representative examples):
  - [compute_tsvector_stats](../c/compute_tsvector_stats.md)

## Notes and Other Information
- Returns the result of lexeme_compare on the respective TrackItem keys
- Enables lexicographic sorting of TrackItem arrays by lexeme content
- Used in PostgreSQL's ANALYZE process for tsvector column statistics
- Part of the text search statistics collection infrastructure
- Located in src/backend/tsearch/ts_typanalyze.c:530-536
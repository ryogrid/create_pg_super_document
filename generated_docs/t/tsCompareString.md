# tsCompareString

## Location
[src/backend/utils/adt/tsvector_op.c:1152-1188](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L1152-L1188)

## Overview
Compares two strings according to tsvector rules with support for both exact matching and prefix matching modes, serving as the fundamental string comparison function for text search operations.

## Definition


## Detailed Description
The  function provides string comparison functionality specifically designed for PostgreSQL's text search (tsvector/tsquery) operations. It supports two comparison modes: exact string comparison and prefix matching. The function handles edge cases like empty strings and implements the comparison logic using memcmp for the overlapping portions.

When in prefix mode (), the function checks if string  is a prefix of string . In exact mode, it performs a standard lexicographic comparison. The function returns standard comparison semantics: negative value if , zero if equal (or prefix match), positive if .

The implementation carefully handles boundary conditions including zero-length strings and ensures consistent behavior across different string lengths.

## Parameters / Member Variables
- **a**: Pointer to first string to compare
- **lena**: Length of first string
- **b**: Pointer to second string to compare
- **lenb**: Length of second string
- **prefix**: Boolean flag indicating comparison mode
  - : Check if  is a prefix of 
  - : Perform exact lexicographic comparison

## Dependencies
- Functions called/Symbols referenced:
  - memcmp: Standard C library memory comparison function
  - Min: Macro to find minimum of two values

- Called from (representative examples):
  - [compareWORD](../c/compareWORD.md): Word comparison in text search parsing
  - [hlfinditem](../h/hlfinditem.md): Text search parsing operations
  - [gin_cmp_tslexeme](../g/gin_cmp_tslexeme.md): GIN index comparison for tsvector lexemes
  - [gin_cmp_prefix](../g/gin_cmp_prefix.md): GIN index prefix comparison
  - QTNodeCompare: Query tree node comparison
  - WordECompareQueryItem: Query ranking comparisons
  - [compareQueryOperand](../c/compareQueryOperand.md): Query operand comparison in ranking
  - [compareentry](../c/compareentry.md): Entry comparison in tsvector operations
  - [silly_cmp_tsvector](../s/silly_cmp_tsvector.md): Tsvector comparison operations
  - compareEntry: Entry comparison in tsvector operations
  - [tsvector_bsearch](tsvector_bsearch.md): Binary search in tsvector
  - [compare_text_lexemes](../c/compare_text_lexemes.md): Text lexeme comparison
  - [checkcondition_str](../c/checkcondition_str.md): Condition checking for text search
  - compareStatWord: Statistical word comparison

## Notes and Other Information
- Returns standard C comparison semantics: <0, 0, >0 for less than, equal, greater than
- In prefix mode, an empty string is considered a prefix of any string
- When strings are equal up to the shorter length but differ in length:
  - Prefix mode: returns 0 only if  is not longer than 
  - Exact mode: shorter string compares as less than longer string
- Widely used throughout PostgreSQL's text search infrastructure
- Critical function for maintaining sorted order in tsvector data structures
- Used by GIN indexing system for efficient text search indexing and querying
- Handles binary-safe comparison (works with any byte sequences, not just text)
- Performance-optimized using memcmp for the core comparison logic
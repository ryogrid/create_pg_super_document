# classify_index_clause_usage

## Location
src/backend/optimizer/path/indxpath.c: 1589 - 1656

## Overview
Constructs a PathClauseUsage structure that describes the WHERE clauses and index predicate clauses used by a given index scan path, classifying clauses for comparison purposes.

## Definition
```c
static PathClauseUsage * classify_index_clause_usage(Path *path, List **clauselist)
```

## Detailed Description
This static function analyzes an index path to extract and classify the WHERE clauses and index predicate clauses it uses. It creates a PathClauseUsage structure that contains both the literal lists of quals and predicates, as well as a bitmapset representation for efficient comparison. The function uses find_indexpath_quals() to recursively extract all quals and predicates from the path, then builds a bitmapset where each bit position corresponds to a unique clause in the global clause list.

To prevent O(N^2) behavior with machine-generated queries that have excessive numbers of clauses, the function implements a safeguard that marks paths with more than 100 total quals and predicates as unclassifiable. This allows the calling code to treat such paths as distinct from all others without expensive detailed analysis.

The function maintains a global clause list across multiple calls to identify distinct clauses using equality comparison, enabling efficient path comparison and deduplication in bitmap AND path selection.

## Parameters / Member Variables
- `path`: Path structure representing the index path to classify
- `clauselist`: Pointer to a List that accumulates all distinct clauses seen across calls; caller must initialize to NIL before the first call in a set

## Dependencies
- Functions called/Symbols referenced:
  - [find_indexpath_quals](../f/find_indexpath_quals.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [find_list_position](../f/find_list_position.md)
  - PathClauseUsage
- Called from (representative examples):
  - [choose_bitmap_and](choose_bitmap_and.md)

## Notes and Other Information
- This is a static function local to indxpath.c
- Implements a safeguard against O(N^2) behavior by marking paths with >100 quals+preds as unclassifiable
- Uses find_list_position() to map clauses to bit positions in the bitmapset representation
- The clauselist parameter is used and expanded across successive calls to maintain a global registry of distinct clauses
- Two clauses are considered the same if they are equal() according to PostgreSQL's equality semantics
- Currently used only within choose_bitmap_and() but designed for potential broader use
- The unclassifiable flag allows calling code to handle complex paths appropriately without expensive analysis
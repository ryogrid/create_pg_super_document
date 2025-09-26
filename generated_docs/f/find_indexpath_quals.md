# find_indexpath_quals

## Location
[src/backend/optimizer/path/indxpath.c:1657-1703](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L1657-L1703)

## Overview
Recursively extracts lists of all index clauses and index predicate conditions used in a Path structure for plain or bitmap index scans, flattening complex AND/OR path structures.

## Definition
```c
static void find_indexpath_quals(Path *bitmapqual, List **quals, List **preds)
```

## Detailed Description
This recursive static function traverses Path structures to extract all the fundamental index clauses and predicate conditions, regardless of their AND/OR organization. It handles three types of Path nodes: BitmapAndPath, BitmapOrPath, and IndexPath. For BitmapAndPath and BitmapOrPath nodes, it recursively processes all child paths. For IndexPath nodes, it extracts the actual index clauses and predicates.

The function flattens the logical structure of the path tree, focusing solely on collecting all base conditions rather than preserving the precise AND/OR semantics. This is useful for analysis tasks that need to know what conditions are involved without caring about their specific logical arrangement.

The function appends results to the provided lists, allowing multiple calls to accumulate results, and creates fresh list cells while reusing the original expression pointers.

## Parameters / Member Variables
- `bitmapqual`: Path structure to analyze (can be BitmapAndPath, BitmapOrPath, or IndexPath)
- `quals`: Pointer to List for accumulating index clause expressions (caller should initialize to NIL)
- `preds`: Pointer to List for accumulating index predicate conditions (caller should initialize to NIL)

## Dependencies
- Functions called/Symbols referenced:
  - [find_indexpath_quals](find_indexpath_quals.md) (recursive calls)
  - [list_concat](../l/list_concat.md)
  - nodeTag
  - [BitmapAndPath](../B/BitmapAndPath.md)
  - [BitmapOrPath](../B/BitmapOrPath.md)
  - [IndexPath](../I/IndexPath.md)
  - [IndexClause](../I/IndexClause.md)
- Called from (representative examples):
  - [classify_index_clause_usage](../c/classify_index_clause_usage.md)
  - [find_indexpath_quals](find_indexpath_quals.md) (recursive self-calls)

## Notes and Other Information
- This is a static function local to indxpath.c
- Recursive function that handles tree-structured bitmap paths
- Does not attempt to preserve AND/OR semantics, only extracts base conditions
- Creates fresh list cells but reuses original expression pointers
- For IndexPath nodes, extracts clauses from indexclauses and predicates from indexinfo->indpred
- Raises ERROR for unrecognized node types to ensure all expected path types are handled
- Results are safe to modify destructively since list cells are freshly allocated
- Essential for path analysis and comparison operations in bitmap index planning
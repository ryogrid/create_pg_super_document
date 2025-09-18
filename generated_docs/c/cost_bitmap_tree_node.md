# cost_bitmap_tree_node

## Location
[src/backend/optimizer/path/costsize.c:1114-1156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L1114-L1156)

## Overview
Extracts cost and selectivity values from bitmap tree nodes, handling different node types (IndexPath, BitmapAndPath, BitmapOrPath) in a unified interface.

## Definition
```c
void cost_bitmap_tree_node(Path *path, Cost *cost, Selectivity *selec)
```

## Detailed Description
This function serves as a utility to extract cost and selectivity information from different types of nodes in a bitmap qualification tree. It acts as a dispatcher that handles three different path types:

1. **IndexPath**: Extracts the index total cost and selectivity, plus adds a small bitmap manipulation cost (0.1 * cpu_operator_cost * rows) to differentiate bitmap scans from regular index scans for single tuple retrieval
2. **BitmapAndPath**: Uses the precomputed total cost and bitmap selectivity from the AND node
3. **BitmapOrPath**: Uses the precomputed total cost and bitmap selectivity from the OR node

The function includes a small bitmap manipulation overhead for IndexPath nodes to ensure that bitmap scans have a slightly higher cost than equivalent index scans when retrieving very few tuples.

## Parameters / Member Variables
- `path`: Pointer to the bitmap tree node (can be IndexPath, BitmapAndPath, or BitmapOrPath)
- `cost`: Output parameter for the extracted cost value
- `selec`: Output parameter for the extracted selectivity value

## Dependencies
- Functions called/Symbols referenced:
  - [IndexPath](../I/IndexPath.md) (struct type)
  - BitmapAndPath (struct type)
  - BitmapOrPath (struct type)
  - nodeTag (function)
  - cpu_operator_cost (global variable)

- Called from:
  - [cost_bitmap_and_node](cost_bitmap_and_node.md) (in costsize.c:1180)
  - [cost_bitmap_or_node](cost_bitmap_or_node.md) (in costsize.c:1225)
  - [compute_bitmap_pages](compute_bitmap_pages.md) (in costsize.c:6422)
  - [choose_bitmap_and](choose_bitmap_and.md) (in indxpath.c:1389, 1390)
  - [path_usage_comparator](../p/path_usage_comparator.md) (in indxpath.c:1502, 1503)

## Notes and Other Information
- This is a utility function that provides a uniform interface for extracting cost/selectivity from different bitmap node types
- Adds a small bitmap manipulation penalty (0.1 * cpu_operator_cost * rows) to IndexPath nodes
- The bitmap manipulation cost helps differentiate bitmap scans from regular index scans in costing
- Includes error handling for unrecognized node types
- Located in src/backend/optimizer/path/costsize.c:1114-1156
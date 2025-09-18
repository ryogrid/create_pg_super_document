# findeq

## Location
src/backend/utils/adt/tsquery_rewrite.c: 35 - 205

## Overview
The `findeq` function performs pattern matching and replacement within TSQuery tree nodes, searching for a specific subtree pattern and replacing matching portions with a substitution subtree.

## Definition
```c
static QTNode *findeq(QTNode *node, QTNode *ex, QTNode *subs, bool *isfind)
```

## Detailed Description
This function is a core component of PostgreSQL's TSQuery rewriting system. It recursively examines a query tree node to find matches with a given example pattern. When a match is found, it replaces the matching portion with a substitution node. The function handles both exact matches (when the entire node matches the example) and subset matches (when a subset of the node's children match the example pattern).

The function implements sophisticated matching logic for different node types:
- For operator nodes (QI_OPR): Performs exact matching when child counts are equal, or subset matching for associative/commutative operators (AND/OR) when the node has more children than the example
- For value nodes (QI_VAL): Performs direct value comparison using CRC checksums

The function sets the QTN_NOCHANGE flag on successfully modified nodes to prevent redundant recursive processing.

## Parameters / Member Variables
- `node`: The current query tree node being examined for matches
- `ex`: The example/pattern node to search for within the current node
- `subs`: The substitution node to replace matched patterns (can be NULL for deletion)
- `isfind`: Output parameter set to true if a replacement was made

## Dependencies
- Functions called/Symbols referenced:
  - QTNEq (tree node equality comparison)
  - QTNFree (tree node memory deallocation)
  - [QTNCopy](../Q/QTNCopy.md) (tree node deep copy)
  - QTNodeCompare (tree node comparison for sorting)
  - QTNSort (tree node sorting)
- Called from (representative examples):
  - [dofindsubquery](../d/dofindsubquery.md)

## Notes and Other Information
- The function relies on pre-sorted child nodes for efficient subset matching in associative/commutative operations
- Uses signature-based filtering for performance optimization before detailed comparison
- Handles the QTN_NOCHANGE flag to avoid redundant processing in recursive scenarios
- The subset matching algorithm works only for AND/OR operators due to their commutative/associative properties
- Memory management is carefully handled with proper cleanup of replaced nodes and copying of substitution nodes
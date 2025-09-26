# QTNode

## Location
[src/include/tsearch/ts_utils.h:234-242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/ts_utils.h#L234-L242)

## Overview
QTNode is a tree node structure used internally for manipulating and processing tsquery expressions in PostgreSQL's full-text search system.

## Definition
```c
typedef struct QTNode
{
    QueryItem  *valnode;    /* pointer to QueryItem for this node */
    uint32      flags;      /* processing flags */
    int32       nchild;     /* number of child nodes */
    char       *word;       /* operand word string */
    uint32      sign;       /* signature for optimization */
    struct QTNode **child;  /* array of pointers to child nodes */
} QTNode;
```

## Detailed Description
QTNode represents a node in a tree structure used for processing tsquery expressions. It provides a more convenient tree-based representation than the flat QueryItem array format used for storage. This structure is primarily used during query rewriting, optimization, comparison operations, and conversion between different tsquery representations.

The tree structure allows for recursive operations on queries, such as normalization, rewriting, comparison, and transformation. Each node can represent either an operator (with child nodes) or an operand (leaf node with a word). The structure includes optimization fields like signatures for faster comparisons and flags for processing state.

QTNode trees are typically created from flat TSQuery representations and can be converted back to the storage format when processing is complete. This design separates the efficient storage format from the convenient manipulation format.

## Parameters / Member Variables
- `valnode`: Pointer to the corresponding QueryItem structure containing the actual query data
- `flags`: Bit flags used during processing for marking nodes, tracking state, or optimization purposes
- `nchild`: Number of child nodes in the tree structure  
- `word`: String containing the operand word for leaf nodes (NULL for operator nodes)
- `sign`: Signature value used for query optimization and fast comparisons
- `child`: Array of pointers to child QTNode structures (for operator nodes)

## Dependencies
- Functions that create/manipulate QTNode trees:
  - QT2QTN (convert QueryItem to QTNode)
  - QTN2QT (convert QTNode back to QueryItem)
  - QTNFree (free QTNode tree memory)
  - QTNCopy (copy QTNode tree)
  - QTNSort (sort child nodes)
  - QTNEq (compare QTNode trees for equality)
  - QTNTernary (process ternary operators)
  - QTNBinary (process binary operators)
  - QTNClearFlags (clear processing flags)
- Used by query operations:
  - tsquery_numnode
  - join_tsqueries
  - tsquery_and, tsquery_or, tsquery_not
  - tsquery_phrase_distance
  - CompareTSQ
- Used by query rewriting functions:
  - findeq
  - dofindsubquery
  - findsubquery
  - tsquery_rewrite_query
  - tsquery_rewrite
- Related types:
  - QueryItem (union containing query data)
  - QueryOperator (operator-specific data)
  - QueryOperand (operand-specific data)

## Notes and Other Information
- Part of PostgreSQL's internal tsquery processing infrastructure
- Provides tree-based representation for easier manipulation than flat QueryItem arrays
- Used extensively in query rewriting and optimization operations
- Memory management requires careful handling of the child array and word strings
- The tree structure supports recursive algorithms for query processing
- Signature field enables fast query comparison and optimization
- Flags field supports various processing states and optimizations during tree traversal
- Conversion functions allow switching between storage format (QueryItem arrays) and manipulation format (QTNode trees)
# NODE

## Location
[src/backend/utils/adt/tsquery_cleanup.c:22-27](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_cleanup.c#L22-L27)

## Overview
NODE is a binary tree structure used internally in PostgreSQL's text search (tsquery) cleanup functionality to represent and manipulate query trees during stopword removal and NOT operator cleanup operations.

## Definition

```c
typedef struct NODE
{
	struct NODE *left;
	struct NODE *right;
	QueryItem  *valnode;
} NODE;
```
## Detailed Description
The NODE structure serves as a fundamental building block for creating binary tree representations of text search queries (TSQuery) during cleanup operations. It is specifically used in the tsquery_cleanup.c module to transform flat QueryItem arrays into tree structures that can be more easily manipulated for removing stopwords and cleaning up NOT operators.

Each NODE represents a single element in the query tree, where:
- Internal nodes typically represent operators (AND, OR, NOT)
- Leaf nodes represent operands (search terms)
- The tree structure mirrors the logical structure and precedence of the original query

The NODE structure enables recursive traversal and modification of query trees, allowing cleanup algorithms to efficiently identify and remove unwanted elements while preserving the logical structure of the remaining query.

## Parameters / Member Variables
- : Pointer to the left child NODE in the binary tree structure. For binary operators (AND, OR), this represents the left operand. For unary operators (NOT), this is typically NULL.
- : Pointer to the right child NODE in the binary tree structure. This represents the right operand for binary operators or the single operand for unary operators like NOT.
- : Pointer to the actual QueryItem that this NODE represents. This contains the operator or operand data from the original TSQuery structure.

## Dependencies
- Functions called/Symbols referenced:
  - QueryItem (referenced as pointer type for valnode member)
- Called from (representative examples):
  - [maketree](../m/maketree.md) (creates NODE instances from QueryItem arrays)
  - [plaintree](../p/plaintree.md) (converts NODE trees back to flat QueryItem arrays)
  - [freetree](../f/freetree.md) (recursively frees NODE tree structures)
  - [clean_NOT_intree](../c/clean_NOT_intree.md) (performs NOT cleanup operations on NODE trees)
  - [clean_stopword_intree](../c/clean_stopword_intree.md) (removes stopwords from NODE trees)
  - [calcstrlen](../c/calcstrlen.md) (calculates string length requirements during tree processing)
  - cleanq_tstopwords (main cleanup function that orchestrates tree operations)

## Notes and Other Information
- [NODE](NODE.md) is a local typedef defined specifically in tsquery_cleanup.c and is not exposed in any header files
- The structure is designed for temporary use during query cleanup operations and is not part of the persistent storage format
- Memory management for NODE structures is handled through PostgreSQL's palloc/pfree system
- The binary tree representation allows for efficient recursive algorithms to process complex query structures
- [NODE](NODE.md) trees are created from flat QueryItem arrays and then converted back to flat format after cleanup operations are complete
- Stack overflow protection is implemented in functions that recursively process NODE trees (via check_stack_depth())
# QueryOperator

## Location
[src/include/tsearch/ts_type.h:202-212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/ts_type.h#L202-L212)

## Overview
QueryOperator represents operator nodes in PostgreSQL's tsquery parse tree, storing information about logical operations (AND, OR, NOT, PHRASE) that combine search terms in text search queries.

## Definition
```c
typedef struct
{
    QueryItemType type;
    int8        oper;           /* OP_NOT, OP_AND, OP_OR, or OP_PHRASE */
    int16       distance;       /* distance between args for OP_PHRASE */
    uint32      left;           /* pointer to left operand. Right operand is
                                 * item + 1, left operand is placed
                                 * item+item->left */
} QueryOperator;
```

## Detailed Description
QueryOperator is a fundamental component of PostgreSQL's text search query representation (tsquery). It represents operator nodes in the abstract syntax tree of a text search query, enabling complex boolean and proximity operations on search terms.

The structure supports four types of operations:
- OP_NOT (1): Logical negation of the operand
- OP_AND (2): Logical AND between left and right operands  
- OP_OR (3): Logical OR between left and right operands
- OP_PHRASE (4): Proximity search with specified distance between terms

The layout uses a clever encoding scheme where the right operand is always at the next array position (item + 1), while the left operand is located at a relative offset specified by the left field. This allows efficient tree traversal during query evaluation.

For phrase operations, the distance field specifies the maximum allowed distance between the two terms, enabling proximity searches like "word1 <-> word2" for adjacent terms or "word1 <N> word2" for terms within N positions.

## Parameters / Member Variables
- `type`: QueryItemType indicating this is an operator node (vs. operand node)
- `oper`: Operation type (OP_NOT=1, OP_AND=2, OP_OR=3, OP_PHRASE=4)
- `distance`: For OP_PHRASE operations, the maximum distance allowed between terms (0 for adjacent)
- `left`: Relative offset to the left operand (right operand is always at next position)

## Dependencies
- Functions called/Symbols referenced:
  - QueryItemType (discriminator for union type)
  - Used within QueryItem union alongside QueryOperand
- Used by (representative examples):
  - [pushOperator](../p/pushOperator.md) (building query parse trees)
  - [findoprnd_recurse](../f/findoprnd_recurse.md) (traversing operand trees) 
  - [QTNodeCompare](QTNodeCompare.md) (comparing query tree nodes)
  - QO_PRIORITY (getting operator precedence)

## Notes and Other Information
- Part of the QueryItem union, enabling polymorphic query tree nodes
- Operator precedence: NOT > PHRASE > AND > OR (defined in tsearch_op_priority array)
- The encoding scheme enables compact tree representation in linear arrays
- Phrase distance of 0 means terms must be adjacent (default <-> operator)
- Negative distances are not supported (phrase operations are always forward-looking)
- Structure is designed for 4-byte alignment to match TSQuery requirements
- The left offset encoding allows arbitrary tree shapes while maintaining array-based storage
# clean_stopword_intree

## Location
src/backend/utils/adt/tsquery_cleanup.c: 238 - 362

## Overview
Recursively removes stopword nodes (QI_VALSTOP) from a TSQuery tree while properly adjusting phrase operator distances to maintain semantic correctness.

## Definition


## Detailed Description
The `clean_stopword_intree` function performs sophisticated cleanup of TSQuery trees by removing stopword nodes and handling the complex distance adjustments required for phrase operators. When stopwords are removed from phrase expressions, the distances between remaining words must be recalculated to preserve the original semantic meaning of the query.

The function handles several scenarios:
- **Simple stopwords**: QI_VALSTOP nodes are removed entirely
- **NOT operators**: Cleaned recursively, removed if child disappears
- **Phrase operators**: Distance calculations are adjusted when operands contain stopwords
- **Other operators**: AND/OR operators are simplified when operands are removed

The most complex aspect is managing phrase operator distances when stopwords are removed. The function uses `ladd` and `radd` parameters to bubble up distance adjustments to parent phrase operators, ensuring that the final query maintains the correct positional relationships between non-stopword terms.

## Parameters / Member Variables
- `node`: Root node of the subtree to clean
- `ladd`: Output parameter - distance to add to phrase operators to the left of this node
- `radd`: Output parameter - distance to add to phrase operators to the right of this node

## Dependencies
- Functions called/Symbols referenced:
  - `check_stack_depth`: Prevents stack overflow in recursive calls
  - [clean_stopword_intree](clean_stopword_intree.md): Recursive self-call for subtrees
  - [freetree](../f/freetree.md): Frees memory for removed tree nodes
  - [pfree](../p/pfree.md): Frees individual nodes
  - `QI_VAL`, `QI_VALSTOP`, `QI_OPR`: Query item type constants
  - `OP_NOT`, `OP_PHRASE`: Operator type constants
  - `NODE`: Tree node structure type

- Called from (representative examples):
  - [cleanup_tsquery_stopwords](cleanup_tsquery_stopwords.md): Main stopword cleanup function in tsquery_cleanup.c:404
  - Self-recursive calls for tree traversal

## Notes and Other Information
- This is a static (internal) function used within the TSQuery cleanup pipeline
- The function implements sophisticated phrase distance adjustment logic to handle cases like 'a <-> b' where 'a' is a stopword becoming just 'b'
- Distance calculations consider complex nested phrase scenarios and properly propagate adjustments up the tree
- The function handles edge cases where entire subtrees may consist only of stopwords
- Memory management is carefully handled with proper cleanup of removed nodes
- Stack depth checking prevents potential stack overflow in deeply nested queries
- The distance adjustment algorithm only works optimally for adjacent phrase operators; some complex nested cases may not be handled perfectly due to TSQuery structure limitations
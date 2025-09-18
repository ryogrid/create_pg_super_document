# findsubquery

## Location
[src/backend/utils/adt/tsquery_rewrite.c:267-279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_rewrite.c#L267-L279)

## Overview
The `findsubquery` function serves as the public interface for TSQuery tree substitution operations, providing a clean wrapper around the recursive tree rewriting functionality.

## Definition
```c
QTNode *findsubquery(QTNode *root, QTNode *ex, QTNode *subs, bool *isfind)
```

## Detailed Description
This function acts as the main entry point for performing pattern-based substitutions throughout a TSQuery tree. It provides a simplified interface that encapsulates the complexity of the recursive tree traversal and substitution logic implemented in `dofindsubquery`. The function ensures that the caller can easily initiate tree rewriting operations while maintaining clean separation between the public API and internal implementation details.

The function serves as a thin wrapper that:
- Initializes the substitution process with proper state management
- Delegates the actual work to the recursive `dofindsubquery` function
- Provides optional feedback about whether any substitutions were performed
- Returns the potentially modified tree structure

This design allows for both simple substitution operations (where the caller doesn't need feedback) and more complex scenarios where knowing whether changes occurred is important for subsequent processing.

## Parameters / Member Variables
- `root`: The root node of the TSQuery tree to be processed for substitutions
- `ex`: The example/pattern node to search for throughout the tree
- `subs`: The substitution node to replace matched patterns (can be NULL for deletion)
- `isfind`: Optional output parameter that receives true if any substitution was made (can be NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - [dofindsubquery](../d/dofindsubquery.md) (performs the actual recursive substitution work)
- Called from (representative examples):
  - [tsquery_rewrite_query](../t/tsquery_rewrite_query.md)
  - [tsquery_rewrite](../t/tsquery_rewrite.md)
  - PG_GETARG_TSQUERYSIGN

## Notes and Other Information
- Requires that both root and ex trees have been preprocessed through QTNTernary and QTNSort for reliable pattern matching
- The function maintains the contract that the input tree structure is preserved if no matches are found
- Provides an optional mechanism for callers to determine if the tree was actually modified
- Acts as a stable public interface that isolates callers from internal implementation changes in the recursive matching logic
- The returned tree may be the same as the input tree (if no changes) or a completely reconstructed tree (if substitutions occurred)
# setNamespaceVisibilityForRTE

## Location
[src/backend/parser/parse_merge.c:415-432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_merge.c#L415-L432)

## Overview
A static utility function that sets the visibility flags for a specific Range Table Entry (RTE) within the parser namespace, controlling whether the relation and its columns can be referenced during expression parsing.

## Definition
```c
static void setNamespaceVisibilityForRTE(List *namespace, RangeTblEntry *rte,
                                          bool rel_visible, bool cols_visible)
```

## Detailed Description
This function searches through the parser's namespace list to find the ParseNamespaceItem corresponding to the given RangeTblEntry and updates its visibility flags. The visibility flags control:

- **rel_visible**: Whether the relation itself can be referenced (affects relation-level operations and qualified column references)
- **cols_visible**: Whether columns from the relation can be referenced (affects unqualified column references and column visibility)

The function performs a linear search through the namespace list to locate the matching RTE, then updates the visibility flags for that namespace item. This is a fundamental operation in PostgreSQL's parser that controls name resolution and scoping rules.

## Parameters / Member Variables
- `namespace`: List of ParseNamespaceItem entries representing the current namespace scope
- `rte`: The Range Table Entry for which visibility should be modified
- `rel_visible`: Boolean flag indicating whether the relation should be visible for referencing
- `cols_visible`: Boolean flag indicating whether columns from the relation should be visible

## Dependencies
- Functions called/Symbols referenced:
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md) (struct type)
  - Standard list iteration functions (foreach, lfirst)
- Called from (representative examples):
  - [setNamespaceForMergeWhen](setNamespaceForMergeWhen.md) (multiple calls to control visibility for different MERGE action types)

## Notes and Other Information
- This is a static helper function used exclusively within the MERGE statement parsing context
- The function assumes that the RTE exists in the namespace - it performs no error checking for missing RTEs
- The linear search approach is acceptable since namespace lists are typically small during parsing
- The visibility flags directly affect how the parser resolves column references and relation names
- Different combinations of rel_visible and cols_visible flags provide fine-grained control over name resolution
- Breaking out of the loop early after finding the matching RTE provides minor performance optimization
- This function is crucial for implementing the complex visibility rules required by MERGE statement semantics
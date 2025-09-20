# substitute_phv_relids

## Location
[src/backend/optimizer/prep/prepjointree.c:4009-4036](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L4009-L4036)

## Overview  
Entry point function that substitutes relation IDs in PlaceHolderVars throughout a query tree or expression tree.

## Definition

```c
static void
substitute_phv_relids(Node *node, int varno, Relids subrelids)
```
## Detailed Description
This function serves as the main entry point for substituting relation IDs in PlaceHolderVar nodes. It initializes a context structure with the substitution parameters and then invokes a tree walker to perform the actual substitution work throughout the entire tree structure.

The function is designed to handle both complete Query nodes and bare expression trees, using  which automatically determines the appropriate traversal method based on the root node type.

This function is typically used during query optimization when append relations (inheritance hierarchies or partitioned tables) are being processed, and references to parent tables need to be replaced with references to their constituent child tables.

## Parameters / Member Variables
- : The root node of the query tree or expression tree to process
- : The relation ID that should be replaced in any PlaceHolderVars
- : The set of relation IDs that should replace varno

## Dependencies  
- Functions called/Symbols referenced:
  - substitute_phv_relids_context (context structure for walker)
  - query_or_expression_tree_walker (generic tree traversal function)
  - [substitute_phv_relids_walker](substitute_phv_relids_walker.md) (performs the actual substitution work)
- Called from (representative examples):
  - [pull_up_simple_subquery](../p/pull_up_simple_subquery.md) (in prepjointree.c:1399)
  - [remove_result_refs](../r/remove_result_refs.md) (in prepjointree.c:3811) 
  - [fix_append_rel_relids](../f/fix_append_rel_relids.md) (in prepjointree.c:4064)

## Notes and Other Information
- This function is static and only used within prepjointree.c
- Acts as a convenient wrapper around the more complex walker implementation
- Handles both Query nodes and bare expression trees automatically
- Part of the append relation processing system in PostgreSQL query optimization
- The actual tree modification is performed by substitute_phv_relids_walker
- Used during query rewriting phases where table references need to be updated due to inheritance or partitioning expansion
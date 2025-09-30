# pull_varnos

## Location
[src/backend/optimizer/util/var.c:108-133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L108-L133)

## Overview
Extracts all distinct variable range table numbers (varnos) present in a parse tree, considering only level-zero rtable entries and outer-join nulling relations.

## Definition

```c
Relids
pull_varnos(PlannerInfo *root, Node *node)
```
## Detailed Description
The  function creates a set of all the distinct varnos present in a parsetree, focusing specifically on varnos that reference level-zero rtable entries. It also includes outer-join relids mentioned in  and  fields within the parse tree.

This function is designed to work with not-yet-planned expressions and handles special cases like bare SubLinks (requiring recursion to look for uplevel references) and completed SubPlans (only examining parameters passed to the subplan). The function uses a walker pattern to traverse the expression tree systematically.

## Parameters / Member Variables
- : PlannerInfo pointer that can be NULL if PlaceHolderVar processing is not required
- : The Node to analyze for variable range table numbers

## Dependencies
- Functions called/Symbols referenced:
  - [pull_varnos_context](pull_varnos_context.md) (struct used for walker context)
  - query_or_expression_tree_walker
  - [pull_varnos_walker](pull_varnos_walker.md)
- Called from (representative examples):
  - [cost_incremental_sort](../c/cost_incremental_sort.md)
  - [get_eclass_for_sort_expr](../g/get_eclass_for_sort_expr.md)
  - [match_saopclause_to_indexcol](../m/match_saopclause_to_indexcol.md)
  - [join_is_removable](../j/join_is_removable.md)
  - [make_outerjoininfo](../m/make_outerjoininfo.md)
  - [distribute_qual_to_rels](../d/distribute_qual_to_rels.md)

## Notes and Other Information
- Designed for use on not-yet-planned expressions, making it suitable for early query planning phases
- Returns a Relids bitmapset containing all discovered varnos
- The function must handle both Query nodes and bare expression trees appropriately
- Special handling for SubLinks vs SubPlans reflects different stages of query planning
- The inclusion of nulling relations is important for proper outer join semantics

## Simplified Source

```c
Relids
pull_varnos(PlannerInfo *root, Node *node)
{
    pull_varnos_context context;

    // Initialize walker context
    context.varnos = NULL;
    context.root = root;
    context.sublevels_up = 0;

    // Walk the expression tree to collect varnos
    query_or_expression_tree_walker(node,
                                    pull_varnos_walker,
                                    (void *) &context,
                                    0);

    return context.varnos;
}
```
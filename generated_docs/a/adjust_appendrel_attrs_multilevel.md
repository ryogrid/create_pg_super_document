# adjust_appendrel_attrs_multilevel

## Location
[src/backend/optimizer/util/appendinfo.c:521-553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/appendinfo.c#L521-L553)

## Overview
Handles variable translation through multiple levels of inheritance hierarchy, recursively applying transformations from a top-level parent down to a deeply nested child relation.

## Definition

```c
Node *
adjust_appendrel_attrs_multilevel(PlannerInfo *root, Node *node,
								  RelOptInfo *childrel,
								  RelOptInfo *parentrel)
```
## Detailed Description
This function manages the complex scenario where a child relation is separated from its ultimate parent by multiple inheritance levels. It recursively traverses up the inheritance hierarchy, applying variable translations at each level until it reaches the specified parent relation. The function ensures that expressions referencing variables in ancestor relations are properly translated to reference the corresponding variables in the target child relation, handling the multi-step transformation that may be required in deep inheritance hierarchies.

## Parameters / Member Variables
- : PlannerInfo containing planning context and relation information
- : The expression tree node to be transformed  
- : The target child relation (leaf level in the inheritance hierarchy)
- : The source parent relation (may be several levels up the hierarchy)

## Dependencies
- Functions called/Symbols referenced:
  - [adjust_appendrel_attrs_multilevel](adjust_appendrel_attrs_multilevel.md) (recursive calls for multi-level traversal)
  - [find_appinfos_by_relids](../f/find_appinfos_by_relids.md) (locates AppendRelInfo structures by relation IDs)
  - [adjust_appendrel_attrs](adjust_appendrel_attrs.md) (performs single-level variable translation)
  - [pfree](../p/pfree.md) (frees allocated memory)
- Called from (representative examples):
  - [generate_join_implied_equalities_broken](../g/generate_join_implied_equalities_broken.md)
  - [add_child_rel_equivalences](add_child_rel_equivalences.md)
  - [grouping_planner](../g/grouping_planner.md)
  - [get_translated_update_targetlist](../g/get_translated_update_targetlist.md)

## Notes and Other Information
- Recursively processes inheritance hierarchy by working from child up to ultimate parent
- Validates that the child relation is actually descended from the specified parent
- Uses find_appinfos_by_relids to locate the appropriate AppendRelInfo mappings
- Properly manages memory by freeing temporary AppendRelInfo arrays
- Essential for complex inheritance scenarios where multiple levels of translation are needed
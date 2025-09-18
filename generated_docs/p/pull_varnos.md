# pull_varnos

## Location
src/backend/optimizer/util/var.c: 108 - 133

## Overview
Extracts all distinct variable range table numbers (varnos) present in a parse tree, considering only level-zero rtable entries and outer-join nulling relations.

## Definition


## Detailed Description
The  function creates a set of all the distinct varnos present in a parsetree, focusing specifically on varnos that reference level-zero rtable entries. It also includes outer-join relids mentioned in  and  fields within the parse tree.

This function is designed to work with not-yet-planned expressions and handles special cases like bare SubLinks (requiring recursion to look for uplevel references) and completed SubPlans (only examining parameters passed to the subplan). The function uses a walker pattern to traverse the expression tree systematically.

## Parameters / Member Variables
- : PlannerInfo pointer that can be NULL if PlaceHolderVar processing is not required
- : The Node to analyze for variable range table numbers

## Dependencies
- Functions called/Symbols referenced:
  - pull_varnos_context (struct used for walker context)
  - query_or_expression_tree_walker
  - pull_varnos_walker
- Called from (representative examples):
  - cost_incremental_sort
  - get_eclass_for_sort_expr
  - match_saopclause_to_indexcol
  - join_is_removable
  - make_outerjoininfo
  - distribute_qual_to_rels

## Notes and Other Information
- Designed for use on not-yet-planned expressions, making it suitable for early query planning phases
- Returns a Relids bitmapset containing all discovered varnos
- The function must handle both Query nodes and bare expression trees appropriately
- Special handling for SubLinks vs SubPlans reflects different stages of query planning
- The inclusion of nulling relations is important for proper outer join semantics
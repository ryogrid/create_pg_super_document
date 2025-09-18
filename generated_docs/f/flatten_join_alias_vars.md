# flatten_join_alias_vars

## Location
src/backend/optimizer/util/var.c: 744 - 766

## Overview
Replaces Vars that reference JOIN outputs with references to the original relation variables, allowing quals involving such vars to be pushed down.

## Definition
```c
Node *flatten_join_alias_vars(PlannerInfo *root, Query *query, Node *node)
```

## Detailed Description
This function serves as the entry point for flattening join alias variables in query expressions. It handles two main transformations:

1. **Variable Reference Flattening**: Replaces Vars that reference JOIN relation outputs with references to the original base relation variables. This transformation is crucial for enabling predicate pushdown optimization, where quals can be moved closer to base relations.

2. **Whole-row Variable Expansion**: Expands whole-row Vars that reference JOIN relations into RowExpr constructs that explicitly name the individual output Vars. This is necessary because the executor can only handle whole-row Vars when scanning base relations directly.

The function also performs important maintenance tasks:
- Adjusts relid sets in expression nodes to substitute base+OJ rels for join relids
- Preserves varnullingrels information from original Vars when making replacements
- Handles SubLink detection and Query.hasSubLinks field updates
- Manages variable level adjustments for nested subqueries

This function is used both by the optimizer during query planning and by the parser for GROUP BY validity checking. When used by the parser (root = NULL), it avoids creating PlaceHolderVars since the parser deals with simpler expressions.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state; NULL when called from parser
- `query`: The Query structure being processed
- `node`: The expression tree to be flattened

## Dependencies
- Functions called/Symbols referenced:
  - flatten_join_alias_vars_context
  - flatten_join_alias_vars_mutator
- Called from (representative examples):
  - subquery_planner (src/backend/optimizer/plan/planner.c:981)
  - preprocess_expression (src/backend/optimizer/plan/planner.c:1180)
  - pull_up_simple_subquery (src/backend/optimizer/prep/prepjointree.c:1254)
  - parseCheckAggregates (src/backend/parser/parse_agg.c:1181)

## Notes and Other Information
- The function asserts that the top node is not the Query itself, as it is designed to work on expressions or LATERAL subqueries
- When called from the parser, PlaceHolderVar creation is avoided since adjust_standard_join_alias_expression can handle all parser-generated join alias expressions
- The function maintains a context structure to track subquery levels and SubLink insertion status
# contain_agg_clause

## Location
src/backend/optimizer/util/clauses.c: 177 - 182

## Overview
Recursively searches for aggregate function nodes (Aggref/GroupingFunc) within a clause and returns true if any aggregates are found.

## Definition


## Detailed Description
This function provides a convenient wrapper around  to detect the presence of aggregate functions or grouping functions within a given expression clause. It performs a recursive traversal of the node tree to identify Aggref and GroupingFunc nodes. The function is designed to work after sublink reduction to subplans and assumes no subqueries or outer-aggregate references are present in the clause being examined.

## Parameters / Member Variables
- : A Node pointer representing the expression clause to be examined for aggregate functions

## Dependencies
- Functions called/Symbols referenced:
  - [contain_agg_clause_walker](contain_agg_clause_walker.md)
- Called from (representative examples):
  - [get_eclass_for_sort_expr](../g/get_eclass_for_sort_expr.md)
  - [subquery_planner](../s/subquery_planner.md)
  - WindowFuncLists

## Notes and Other Information
- This function should only be used after reduction of sublinks to subplans or in contexts where it's known there are no subqueries
- There must not be outer-aggregate references in the clause
- For handling subqueries, consider using  from rewriteManip.c instead
- Returns a boolean value indicating aggregate presence
- Part of the aggregate-function clause manipulation utilities in the PostgreSQL optimizer
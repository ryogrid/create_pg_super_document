# check_ungrouped_columns_walker

## Location
src/backend/parser/parse_agg.c: 1295 - 1482

## Overview
Core recursive tree walker that implements PostgreSQL's GROUP BY validation logic by examining each node for ungrouped variables and enforcing SQL aggregation rules.

## Definition


## Detailed Description
This function implements the sophisticated logic for validating GROUP BY compliance:

1. **Constant and parameter handling**: Immediately accepts constants and parameters as always valid
2. **Aggregate function processing**: 
   - For same-level aggregates, validates direct arguments but allows normal arguments and ORDER BY clauses
   - Skips higher-level aggregates entirely (cannot contain relevant variables)
   - Continues checking lower-level aggregates
3. **GroupingFunc handling**: Properly handles GROUPING() functions at appropriate levels
4. **Complex expression matching**: When non-variable GROUP BY expressions exist, checks if entire subexpressions match GROUP BY items before examining variables within them
5. **Variable validation**:
   - Ignores variables from different query levels
   - Checks simple variable matches against GROUP BY clauses  
   - Performs expensive functional dependency analysis using table constraints as a last resort
   - Maintains func_grouped_rels list to cache functional dependency results
6. **Error generation**: Produces detailed error messages distinguishing between regular ungrouped columns and those in aggregate direct arguments
7. **Recursive traversal**: Handles subquery descent with proper sublevel tracking

The function uses expression_tree_walker and query_tree_walker for efficient tree traversal.

## Parameters / Member Variables
- : Current node being examined in the expression tree
- : Rich context structure containing:
  - Parser state and query information
  - GROUP BY clauses and common variables
  - Functional grouping state and sublevel tracking
  - Special flags for aggregate direct arguments

## Dependencies
- Functions called/Symbols referenced:
  - expression_tree_walker
  - query_tree_walker
  - [check_functional_grouping](check_functional_grouping.md)
  - [equal](../e/equal.md)
  - [list_member_int](../l/list_member_int.md)
  - rt_fetch
  - [get_rte_attribute_name](../g/get_rte_attribute_name.md)
  - lappend_int
- Called from (representative examples):
  - [check_ungrouped_columns](check_ungrouped_columns.md) (entry point)
  - Self-recursion for tree traversal

## Notes and Other Information
- Implements PostgreSQL's sophisticated functional dependency detection using table constraints
- Caches functional dependency results in func_grouped_rels to avoid redundant expensive checks
- Handles aggregate direct arguments specially with detailed error messages for ordered-set aggregates
- Distinguishes between main query ungrouped variables and subquery references to outer variables
- The functional dependency check adds constraints to the query's constraintDeps list for semantic validation
- Part of PostgreSQL's comprehensive SQL standard compliance for GROUP BY semantics
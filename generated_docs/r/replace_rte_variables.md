# replace_rte_variables

## Location
[src/backend/rewrite/rewriteManip.c:1346-1392](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L1346-L1392)

## Overview
Finds all Vars in an expression tree that reference a particular RTE (Range Table Entry) and replaces them with substitute expressions obtained from a caller-supplied callback function.

## Definition


## Detailed Description
This function performs a tree walk through PostgreSQL expression trees to find all Var nodes that reference a specific range table entry (identified by target_varno at a specific sublevel) and replaces them using a user-provided callback function. It's a key utility in PostgreSQL's query rewriting system, particularly useful for operations like view expansion, subquery pullup, and other query transformations.

The function handles SubLink tracking carefully - when a replacement introduces new SubLinks into the expression tree, it updates the hasSubLinks flag appropriately. This is crucial for maintaining query metadata consistency during transformations.

The function uses the query_or_expression_tree_mutator infrastructure to recursively process both Query nodes and bare expression trees, with special handling to avoid incrementing sublevels_up when starting with a Query node.

## Parameters / Member Variables
- : The expression tree or Query to process for variable replacement
- : The range table entry number whose variables should be replaced
- : The sublevel depth at which to look for the target RTE
- : User-provided function that determines the replacement expression for each matching Var
- : Additional context data passed to the callback function
- : Pointer to hasSubLinks flag of containing Query (NULL if not in a Query context)

## Dependencies
- Functions called/Symbols referenced:
  - query_or_expression_tree_mutator
  - [replace_rte_variables_mutator](replace_rte_variables_mutator.md)
  - [replace_rte_variables_context](replace_rte_variables_context.md)
  - replace_rte_variables_callback
- Called from (representative examples):
  - [pullup_replace_vars](../p/pullup_replace_vars.md)
  - [pullup_replace_vars_subquery](../p/pullup_replace_vars_subquery.md)
  - [ReplaceVarsFromTargetList](../R/ReplaceVarsFromTargetList.md)

## Notes and Other Information
- The function exposes its mutator function and context struct publicly because callbacks often need to recurse directly to the mutator on sub-expressions
- SubLink handling is complex but necessary for maintaining query structure integrity during transformations
- Unlike aggregate and window function tracking, SubLink tracking requires special attention because replacements can introduce new subqueries
- The function can start with either a Query or bare expression tree, with appropriate handling for both cases
- Error handling ensures that if SubLinks are inserted but no place exists to record them, an error is raised
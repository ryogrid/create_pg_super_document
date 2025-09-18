# pull_vars_of_level

## Location
[src/backend/optimizer/util/var.c:335-354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L335-L354)

## Overview
Creates a list of all Vars (and PlaceHolderVars) referencing the specified query level in the given parsetree.

## Definition


## Detailed Description
The  function traverses a parse tree or expression tree to collect all variable references (Var nodes and PlaceHolderVar nodes) that reference a specific query nesting level. This function is commonly used in PostgreSQL's query optimizer to analyze variable dependencies across different query levels, particularly for handling subqueries and lateral references.

The function uses a walker pattern with  to recursively traverse the node tree. It maintains a context structure to track the target level and accumulate found variables. The collected variables are not copied but linked directly into the result list for efficiency.

## Parameters / Member Variables
- : The root node of the parse tree or expression tree to search
- : The target query nesting level to search for (0 = current level, 1 = one level up, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [pull_vars_context](pull_vars_context.md) (context structure)
  - query_or_expression_tree_walker (tree traversal function)
  - [pull_vars_walker](pull_vars_walker.md) (callback function for tree walking)
- Called from (representative examples):
  - [extract_lateral_references](../e/extract_lateral_references.md) (multiple calls in src/backend/optimizer/plan/initsplan.c)

## Notes and Other Information
- The function is designed to handle both Query nodes and bare expression trees
- [Variables](../V/Variables.md) in the returned list are not copied, only linked, so callers should be careful about modifying them
- Used primarily in lateral reference analysis and subquery optimization
- Part of PostgreSQL's variable analysis utilities in the optimizer
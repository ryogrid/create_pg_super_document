# contain_subplans_walker

## Location
[src/backend/optimizer/util/clauses.c:336-369](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L336-L369)

## Overview
A tree-walking function that recursively searches for subplan-related nodes (SubPlan, AlternativeSubPlan, or SubLink) within an expression tree.

## Definition

```c
static bool
contain_subplans_walker(Node *node, void *context)
```
## Detailed Description
This static helper function implements the actual recursive tree traversal logic for detecting subplans within an expression tree. It serves as the worker function for  and follows PostgreSQL's standard tree walker pattern.

The function checks for three types of nodes that indicate the presence of subplans:
1. **SubPlan**: An actual subplan node that represents a subquery execution plan
2. **AlternativeSubPlan**: A node representing multiple alternative subplan execution strategies
3. **SubLink**: A node representing a subquery link that hasn't yet been transformed into a subplan

When any of these node types is encountered, the function immediately returns true and aborts further tree traversal. If none are found at the current node, it continues the recursive search using .

## Parameters / Member Variables
- : The current expression node being examined during tree traversal
- : Context parameter (currently unused, passed as NULL)

## Dependencies
- Functions called/Symbols referenced:
  - expression_tree_walker
  - SubPlan (node type check)
  - AlternativeSubPlan (node type check) 
  - SubLink (node type check)
  - [contain_subplans_walker](contain_subplans_walker.md) (recursive call)
- Called from (representative examples):
  - [contain_subplans](contain_subplans.md)
  - max_parallel_hazard_context
  - [contain_subplans_walker](contain_subplans_walker.md) (recursive)

## Notes and Other Information
- This is a static function, only accessible within the same source file
- Follows PostgreSQL's standard tree walker pattern for expression traversal
- Returns true immediately upon finding any subplan-related node (short-circuit evaluation)
- Uses recursive self-calls through  for complete tree coverage
- Part of the subplan detection infrastructure used during query planning
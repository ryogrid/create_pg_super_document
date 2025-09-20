# replace_correlation_vars_mutator

## Location
[src/backend/optimizer/plan/subselect.c:1875-1918](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L1875-L1918)

## Overview
A tree-walking mutator function that recursively traverses expression trees to replace uplevel correlation variables and expressions with appropriate Param nodes for subquery execution.

## Definition

```c
static Node *
replace_correlation_vars_mutator(Node *node, PlannerInfo *root)
```
## Detailed Description
This function implements the core logic for replacing correlation variables in expression trees. It uses PostgreSQL's expression tree mutator framework to recursively walk through expression nodes and identify uplevel references that need to be converted to execution parameters.

The function handles five specific types of uplevel references:
1. **Var nodes**: Variables referencing parent query levels (varlevelsup > 0)
2. **PlaceHolderVar nodes**: Placeholder variables from parent levels (phlevelsup > 0)  
3. **Aggref nodes**: Aggregate function references from parent levels (agglevelsup > 0)
4. **GroupingFunc nodes**: GROUPING() expressions from parent levels (agglevelsup > 0)
5. **MergeSupportFunc nodes**: Merge support functions (but only outside MERGE commands)

For each type of uplevel reference, it delegates to a specialized replacement function that creates the appropriate Param node. The function uses  to ensure complete traversal of the expression tree, including recursive calls to itself for nested expressions.

## Parameters / Member Variables
- : The current expression node being processed in the tree walk
- : PlannerInfo structure containing the query planning context

## Dependencies
- Functions called/Symbols referenced:
  - : Creates Param replacement for uplevel Var nodes
  - : Creates Param replacement for uplevel PlaceHolderVar nodes
  - : Creates Param replacement for uplevel Aggref nodes
  - : Creates Param replacement for uplevel GroupingFunc nodes
  - : Creates Param replacement for uplevel MergeSupportFunc nodes
  - : PostgreSQL's generic expression tree traversal framework
- Called from (representative examples):
  - : Main entry point that initiates the tree walk
  - : Recursive self-calls during tree traversal

## Notes and Other Information
- Returns the original node if no replacement is needed, or a new Param node if replacement occurs
- Uses PostgreSQL's IsA() macro system for type checking and safe casting
- The function is recursive and handles arbitrarily nested expression structures
- Special handling for MergeSupportFunc ensures they're only replaced outside MERGE commands
- Part of the correlation variable resolution system that enables proper subquery parameter passing
- The mutator pattern ensures that all nodes in complex expression trees are properly processed
- Critical for converting correlated subqueries into parameterized subplans that can be executed efficiently
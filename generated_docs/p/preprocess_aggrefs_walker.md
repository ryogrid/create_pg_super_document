# preprocess_aggrefs_walker

## Location
src/backend/optimizer/prep/prepagg.c: 344 - 379

## Overview
A recursive tree walker function that traverses expression trees to find and preprocess all Aggref (aggregate function reference) nodes.

## Definition
```c
static bool preprocess_aggrefs_walker(Node *node, PlannerInfo *root)
```

## Detailed Description
This function implements a recursive tree walker that traverses PostgreSQL expression trees to locate and process aggregate function references. It follows the standard expression_tree_walker pattern used throughout PostgreSQL for tree traversal.

The function performs the following logic:
1. **Base case handling**: Returns false for NULL nodes
2. **Aggref processing**: When encountering an Aggref node, calls preprocess_aggref to handle the aggregate-specific processing
3. **Skip recursion for aggregates**: After processing an Aggref, returns false to avoid recursing into the aggregate's arguments, as the parser guarantees no nested aggregates exist at the same level
4. **SubLink assertion**: Ensures no SubLink nodes are encountered, as these should be handled elsewhere in planning
5. **Recursive traversal**: For all other node types, continues the tree walk using expression_tree_walker

The walker assumes that the PostgreSQL parser has already validated that:
- No aggregates of the same query level exist within aggregate arguments
- No aggregates exist within direct arguments or filter clauses
- No nested aggregation occurs inappropriately

This optimization allows the walker to skip deep recursion into aggregate sub-expressions, improving performance while maintaining correctness.

## Parameters / Member Variables
- : Current node in the expression tree being visited
- : PlannerInfo structure containing planner context and state information

## Dependencies
- Functions called/Symbols referenced:
  - [preprocess_aggref](preprocess_aggref.md)
  - expression_tree_walker
  - IsA (macro for type checking)
- Called from (representative examples):
  - [preprocess_aggrefs](preprocess_aggrefs.md)
  - [preprocess_aggrefs_walker](preprocess_aggrefs_walker.md) (recursive self-call)

## Notes and Other Information
- This is a static function only accessible within the same source file
- Implements the standard PostgreSQL expression tree walker pattern
- Returns false to continue tree traversal or stop recursion as appropriate
- The function is tail-recursive when encountering Aggref nodes to avoid unnecessary deep recursion
- Relies on parser guarantees about aggregate nesting to optimize traversal
- The Assert for SubLink indicates these should be processed by subquery planning before aggregate preprocessing
- Uses expression_tree_walker for general tree traversal, which handles all PostgreSQL expression node types
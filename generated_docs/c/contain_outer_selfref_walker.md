# contain_outer_selfref_walker

## Location
src/backend/optimizer/plan/subselect.c: 1097 - 1137

## Overview
A recursive tree walker function that traverses query nodes to detect external recursive self-references in CTEs (Common Table Expressions) by examining range table entries and tracking query nesting depth.

## Definition


## Detailed Description
This function performs the actual tree traversal work for detecting external recursive self-references in PostgreSQL query trees. It implements a depth-first search through the query tree structure, maintaining a depth counter to track the current query nesting level. The core logic identifies problematic CTE self-references where a CTE references itself from a query level that is equal to or higher than the CTE's definition level (indicated by ).

The function handles three main node types:
1. **RangeTblEntry nodes**: Checks for CTE self-references with inappropriate nesting levels
2. **Query nodes**: Recursively processes subqueries while properly managing depth tracking
3. **Other expression nodes**: Delegates to standard expression tree walking

The depth tracking is critical because it allows the function to distinguish between legitimate recursive references (within the same query level) and problematic external references (crossing query boundaries).

## Parameters / Member Variables
- : The current Node being examined in the tree traversal
- : Pointer to an Index tracking the current query nesting depth (modified during traversal)

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - query_tree_walker
  - expression_tree_walker
  - QTW_EXAMINE_RTES_BEFORE (flag constant)
  - RTE_CTE (enum value)
- Called from (representative examples):
  - contain_outer_selfref
  - contain_outer_selfref_walker (recursive calls)

## Notes and Other Information
- This is a recursive function that calls itself when processing Query nodes and expression nodes
- The depth parameter is incremented when entering a subquery and decremented when exiting
- Uses the QTW_EXAMINE_RTES_BEFORE flag to ensure range table entries are examined before other query components
- Returns true immediately upon finding the first external self-reference, short-circuiting further traversal
- The function distinguishes between self-references at the same level (allowed) vs. external levels (problematic)
- Static function scope limits visibility to the subselect.c compilation unit
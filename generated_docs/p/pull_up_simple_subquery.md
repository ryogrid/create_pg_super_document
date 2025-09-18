# pull_up_simple_subquery

## Location
src/backend/optimizer/prep/prepjointree.c: 1123 - 1468

## Overview
Performs the complex transformation of pulling up a simple subquery into the parent query by merging range tables, adjusting variable references, and handling various semantic complications.

## Definition


## Detailed Description
This function implements the core mechanics of subquery pull-up transformation. It takes a subquery that has been determined to be "simple" (no aggregation, DISTINCT, etc.) and physically merges it into the parent query by:

1. **Subquery Preprocessing**: Creates a PlannerInfo for the subquery and recursively processes it (pull-up SubLinks, preprocess functions, recursively pull up nested subqueries).

2. **Variable Offset Adjustment**: Adjusts variable numbers in the subquery to account for appending its range table to the parent's range table.

3. **Variable Level Adjustment**: Updates variable sublevel references since upper-level variables are now one level closer to their parent.

4. **Variable Replacement**: Replaces all references to the subquery in the parent query with references to the subquery's target list items, potentially wrapping them in PlaceHolderVars.

5. **Range Table Merging**: Combines the subquery's range table and related metadata with the parent query.

6. **Lateral Reference Handling**: Propagates LATERAL markers to child RTEs that might contain lateral cross-references.

7. **Cleanup**: Handles various housekeeping tasks like updating flags and combining lists.

The function includes extensive safety checks and can abort the pull-up if conditions change during processing.

## Parameters / Member Variables
- : PlannerInfo structure for the parent query
- : RangeTblRef node representing the subquery in the jointree  
- : RangeTblEntry for the subquery being pulled up
- : Reference to lowest containing outer join, or NULL
- : Reference to containing append relation, or NULL

## Dependencies
- Functions called/Symbols referenced:
  -  - Creates modifiable copy of subquery
  -  - Handles empty FROM clauses
  -  - Processes SubLinks within subquery
  -  - Preprocesses function RTEs in subquery
  -  - Recursively processes subquery's subqueries
  -  - Re-validates subquery simplicity
  -  - Flattens join alias variables
  -  - Adjusts variable numbers
  -  - Adjusts variable sublevels
  -  - Performs variable replacement
  -  - Merges range tables
  -  - Gets relation IDs from jointree
  -  - Updates PlaceHolderVar relids
  -  - Fixes AppendRelInfo relids

- Called from (representative examples):
  -  - During recursive subquery processing

## Notes and Other Information
- The function creates a complete PlannerInfo structure for the subquery, duplicating setup from 
- [Variable](../V/Variable.md) replacement requires careful handling of PlaceHolderVars, especially for appendrel members and queries with grouping sets
- LATERAL subqueries require special handling to propagate lateral markers to child RTEs
- The function performs extensive validation and can abort pull-up if the subquery becomes non-simple during processing
- Range table combination preserves all metadata including row marks and permission info
- The original subquery is nulled out in the RTE to avoid waste when the query is later pulled up again
- Returns either the subquery's jointree or a single member if the FromExpr is degenerate
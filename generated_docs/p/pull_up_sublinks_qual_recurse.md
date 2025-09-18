# pull_up_sublinks_qual_recurse

## Location
src/backend/optimizer/prep/prepjointree.c: 637 - 886

## Overview
Recursively processes qualification clauses to identify and transform SubLinks (ANY and EXISTS) into semijoin or anti-semijoin operations.

## Definition


## Detailed Description
This function is the core engine for SubLink transformation during query optimization. It recursively traverses qualification expressions to find SubLink nodes that can be converted into more efficient join operations. The function handles several types of SubLink transformations:

**ANY SubLink Processing**: Converts clauses like "expr op ANY (subquery)" into semijoin operations using convert_ANY_sublink_to_join. The transformation preserves the original semantics while allowing the optimizer to consider join-based execution strategies.

**EXISTS SubLink Processing**: Transforms "EXISTS (subquery)" clauses into semijoins and "NOT EXISTS (subquery)" into anti-semijoins using convert_EXISTS_sublink_to_join.

**Dual Location Support**: The function supports two possible attachment points for pulled-up joins (jtlink1/available_rels1 and jtlink2/available_rels2), allowing flexibility in join placement based on variable references in the SubLink.

**AND Clause Recursion**: Recursively processes AND clauses to find SubLinks at any depth, while stopping at non-AND expressions to preserve proper semantics.

**NOT Clause Handling**: Special handling for "NOT EXISTS" patterns, where the NOT is recognized and passed to the conversion function to create anti-semijoins.

After successful SubLink conversion, the function:
1. Inserts the new JoinExpr into the appropriate location in the jointree
2. Recursively processes the pulled-up subtree with pull_up_sublinks_jointree_recurse
3. Recursively processes any remaining quals in the pulled-up subquery
4. Returns NULL (representing constant TRUE) since the condition is now handled by the join

## Parameters / Member Variables
- : PlannerInfo containing query optimization context and metadata
- : The qualification expression node to process (SubLink, BoolExpr, etc.)
- : Pointer to primary jointree location where new joins should be inserted
- : Set of relation IDs available at jtlink1 location
- : Pointer to secondary jointree location (optional, can be NULL)
- : Set of relation IDs available at jtlink2 location (optional)

## Dependencies
- Functions called/Symbols referenced:
  - [convert_ANY_sublink_to_join](../c/convert_ANY_sublink_to_join.md)
  - [convert_EXISTS_sublink_to_join](../c/convert_EXISTS_sublink_to_join.md)  
  - [pull_up_sublinks_jointree_recurse](pull_up_sublinks_jointree_recurse.md)
  - [is_notclause](../i/is_notclause.md), get_notclausearg
  - [is_andclause](../i/is_andclause.md), make_andclause
  - lappend, lfirst, linitial, list_length
  - IsA macro
  - ANY_SUBLINK, EXISTS_SUBLINK constants
- Called from (representative examples):
  - [pull_up_sublinks_jointree_recurse](pull_up_sublinks_jointree_recurse.md) (in multiple locations for processing join and fromexpr quals)
  - Self-recursive calls for processing AND clauses and pulled-up subquery quals

## Notes and Other Information
- Returns NULL when SubLink is successfully converted, representing constant TRUE
- Maintains original expression structure when SubLinks cannot be converted
- Handles complex nesting scenarios with proper variable scope management
- Critical for subquery decorrelation and semijoin optimization performance
- Must preserve NULL semantics of original SubLink expressions
- Stops recursion at non-AND expressions to avoid semantic changes
- Under NOT clauses, restricts pull-up to only reference the right-hand side of newly created joins
- Supports stacking multiple pulled-up SubLinks in encounter order, relying on subsequent optimization for reordering
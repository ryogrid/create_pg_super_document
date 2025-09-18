# clauselist_apply_dependencies

## Location
src/backend/statistics/dependencies.c: 1014 - 1167

## Overview
Applies functional dependencies to a list of clauses and returns the estimated selectivity by combining per-column selectivities using dependency degrees, accounting for the correlation between attributes.

## Definition


## Detailed Description
This function implements the core logic for applying functional dependencies during selectivity estimation. It processes clauses that are compatible with given dependencies and computes a more accurate combined selectivity than would result from assuming independence.

The algorithm works in several phases:

1. **Attribute Collection**: Extracts all attribute numbers (both implying and implied) from the given dependencies
2. **Individual Selectivity Computation**: Computes per-column selectivity estimates for each attribute using 
3. **Dependency Application**: Combines selectivities using the mathematical formula that accounts for dependency strength

The key mathematical formula used is:


Where  is the degree of dependency. For dependency chains (a->b->c), conditional probabilities are computed:


The function processes dependencies in reverse order to handle dependency chains correctly.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context
- : List of WHERE clauses to estimate selectivity for
- : Relation ID for the target relation
- : Type of join operation
- : Special join information structure
- : Array of functional dependencies to apply
- : Number of dependencies in the array
- : Array mapping clause positions to attribute numbers
- : Output bitmapset of clauses that were estimated

## Dependencies
- Functions called/Symbols referenced:
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_num_members](../b/bms_num_members.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [bms_member_index](../b/bms_member_index.md)
  - [clauselist_selectivity_ext](clauselist_selectivity_ext.md)
  - CLAMP_PROBABILITY
  - [bms_free](../b/bms_free.md)
- Types used:
  - JoinType
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md)
  - MVDependency
- Called from (representative examples):
  - DependencyGenerator
  - [dependencies_clauselist_selectivity](../d/dependencies_clauselist_selectivity.md)

## Notes and Other Information
- Uses Min(P(a), P(b)) instead of P(a) for dependent rows to ensure the result doesn't exceed individual column selectivities
- Processes dependencies in reverse order to handle dependency chains correctly
- Marks all processed clauses in the estimatedclauses bitmapset to avoid double-counting
- The conditional probability formula ensures that dependency chains are handled properly
- Results are clamped using CLAMP_PROBABILITY to ensure valid probability ranges
- Memory management includes proper cleanup of allocated bitmapsets and arrays
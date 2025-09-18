# create_lateral_join_info

## Location
src/backend/optimizer/plan/initsplan.c: 501 - 739

## Overview
Analyzes and establishes lateral dependency relationships between base relations by computing direct and transitive lateral reference sets.

## Definition


## Detailed Description
This function is responsible for building the complete picture of lateral dependencies in a query by examining all base relations and establishing three key sets for each relation:
- **direct_lateral_relids**: Relations directly referenced by LATERAL constructs
- **lateral_relids**: All relations that must be available (direct and indirect dependencies) 
- **lateral_referencers**: Relations that reference this relation laterally

The function operates in several phases:
1. Processes simple lateral references from variables extracted by extract_lateral_references
2. Handles lateral references within PlaceHolderVars, considering their evaluation sites
3. Computes the transitive closure using Warshall's algorithm to capture indirect dependencies
4. Creates reverse mapping to identify which relations are referenced by others

This comprehensive analysis is essential for join ordering, as relations with lateral dependencies must be processed in the correct sequence during plan generation.

## Parameters / Member Variables
- : The PlannerInfo structure containing the query tree and planning state

## Dependencies
- Functions called/Symbols referenced:
  - find_placeholder_info
  - find_base_rel
  - find_base_rel_ignore_join
  - bms_add_member
  - bms_add_members
  - bms_copy
  - bms_intersect
  - bms_is_empty
  - bms_get_singleton_member
  - bms_is_member
  - bms_next_member
- Called from (representative examples):
  - query_planner

## Notes and Other Information
- Only executes if root->hasLateralRTEs is true, providing early exit optimization
- Requires that root->placeholdersFrozen is true to ensure PlaceHolderVar evaluation sites are finalized
- Handles different evaluation scenarios for PlaceHolderVars (baserel vs join evaluation sites)
- Uses transitive closure computation to ensure all indirect lateral dependencies are captured
- Filters lateral references to include only base relations, excluding outer joins from dependency tracking
- Resets root->hasLateralRTEs to false if no actual lateral references are found, optimizing subsequent processing
- The lateral_referencers set enables efficient reverse lookup during join planning
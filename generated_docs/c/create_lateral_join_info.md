# create_lateral_join_info

## Location
[src/backend/optimizer/plan/initsplan.c:501-739](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L501-L739)

## Overview
Analyzes and establishes lateral dependency relationships between base relations by computing direct and transitive lateral reference sets.

## Definition

```c
void
create_lateral_join_info(PlannerInfo *root)
```
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
  - [find_placeholder_info](../f/find_placeholder_info.md)
  - [find_base_rel](../f/find_base_rel.md)
  - [find_base_rel_ignore_join](../f/find_base_rel_ignore_join.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_add_members](../b/bms_add_members.md)
  - [bms_copy](../b/bms_copy.md)
  - [bms_intersect](../b/bms_intersect.md)
  - bms_is_empty
  - [bms_get_singleton_member](../b/bms_get_singleton_member.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_next_member](../b/bms_next_member.md)
- Called from (representative examples):
  - [query_planner](../q/query_planner.md)

## Notes and Other Information
- Only executes if root->hasLateralRTEs is true, providing early exit optimization
- Requires that root->placeholdersFrozen is true to ensure PlaceHolderVar evaluation sites are finalized
- Handles different evaluation scenarios for PlaceHolderVars (baserel vs join evaluation sites)
- Uses transitive closure computation to ensure all indirect lateral dependencies are captured
- Filters lateral references to include only base relations, excluding outer joins from dependency tracking
- Resets root->hasLateralRTEs to false if no actual lateral references are found, optimizing subsequent processing
- The lateral_referencers set enables efficient reverse lookup during join planning
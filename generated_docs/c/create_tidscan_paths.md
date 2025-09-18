# create_tidscan_paths

## Location
src/backend/optimizer/path/tidpath.c: 487 - 556

## Overview
Creates and adds TID scan paths to a relation's path list, including both regular TID scans and parameterized TID scans based on CTID equality conditions.

## Definition
void create_tidscan_paths(PlannerInfo *root, RelOptInfo *rel)

## Detailed Description
This function is responsible for generating all possible TID (Tuple Identifier) scan paths for a given relation during query planning. TID scans are specialized access methods that directly locate table rows using their physical tuple identifiers (CTIDs), which can be much faster than traditional sequential or index scans when CTID values are known.

The function operates in three main phases:

1. **Plain TID Scans**: Examines the relation's base restriction clauses to find direct CTID qualifications (e.g., WHERE ctid = '(0,1)') and creates unparameterized TID scan paths.

2. **TID Range Scans**: Looks for CTID range conditions in the restriction clauses and creates TidRangePath objects for efficient range-based tuple access.

3. **Parameterized TID Scans**: Generates parameterized paths for join conditions involving CTIDs, handling both equivalence class relationships and loose join qualifications.

The function is particularly important for optimizing queries that involve CTID comparisons between tables or explicit CTID filtering, enabling the planner to choose direct tuple access over more expensive scan methods.

## Parameters / Member Variables
- : PlannerInfo structure containing the overall query planning context and state
- : RelOptInfo structure representing the relation for which TID scan paths are being generated

## Dependencies
- Functions called/Symbols referenced:
  - TidQualFromRestrictInfoList (extracts TID qualifications from restriction clauses)
  - create_tidscan_path (creates a standard TID scan path)
  - add_path (adds a path to the relation's path list)
  - TidRangeQualFromRestrictInfoList (extracts TID range qualifications)
  - create_tidrangescan_path (creates a TID range scan path)
  - generate_implied_equalities_for_column (generates implied equalities from equivalence classes)
  - ec_member_matches_ctid (callback function to match CTID equivalence members)
  - BuildParameterizedTidPaths (builds parameterized TID paths from join clauses)
- Called from (representative examples):
  - set_plain_rel_pathlist (main path generation function in allpaths.c)

## Notes and Other Information
- This function only adds paths to the relation's pathlist; it doesn't modify existing paths
- TID scans are most effective when CTID values are provided as constants or through joins
- The function handles LATERAL references by including lateral_relids in required_outer parameters
- Parameterized TID paths are crucial for optimizing joins where CTID equality conditions exist
- EquivalenceClass processing enables recognition of CTID equalities that emerged from query transformation
- The function distinguishes between exact TID matches and range-based TID access patterns
- Generated paths compete with other access methods during the cost-based path selection process
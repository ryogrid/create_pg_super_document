# make_partition_pruneinfo

## Location
src/backend/partitioning/partprune.c: 220 - 391

## Overview
Builds a PartitionPruneInfo structure that enables additional partition pruning during query execution, returning NULL when partition pruning would be ineffective.

## Definition


## Detailed Description
This function constructs a PartitionPruneInfo data structure that the executor can use to perform runtime partition pruning. It analyzes the provided subpaths to identify partition child relations and their parent partitioned relations, then builds pruning information for each relevant partition hierarchy.

The function works by:
1. Scanning subpaths to identify partition child relations and their partitioned parent relations
2. Creating a mapping from partition child relation IDs to subplan indexes
3. Traversing up partition hierarchies to collect all relevant partitioned relations
4. Building PartitionedRelPruneInfo structures for each top-level partitioned relation
5. Identifying subplans that don't belong to any partitioned relation (for UNION ALL with non-partitioned tables)

The function restricts parent partitioned relations to be either the parentrel or children of parentrel to ensure that prunequal clauses can be properly translated.

## Parameters / Member Variables
- : PlannerInfo containing global planning state and relation information
- : RelOptInfo for the appendrel being processed
- : List of scan paths for the appendrel's child relations
- : List of potential pruning qualification clauses applicable to the appendrel

## Dependencies
- Functions called/Symbols referenced:
  - add_part_relids
  - make_partitionedrel_pruneinfo
  - find_base_rel
  - IS_PARTITIONED_REL
  - bms_add_member
  - bms_join
  - bms_num_members
  - bms_add_range
  - bms_del_members
- Called from (representative examples):
  - create_append_plan
  - create_merge_append_plan

## Notes and Other Information
- Returns NULL when no useful runtime pruning can be performed
- Handles multi-level partitioning hierarchies by traversing up to topmost partitioned parents
- Creates a complement bitmapset for subplans that don't belong to any partitioned relation
- Uses 1-based indexing in the relid_subplan_map for convenience (zero represents unfilled entries)
- Properly handles partitionwise joins of multi-level partitioning trees where parentrel may be an intermediate partitioned table
# make_partitionedrel_pruneinfo

## Location
src/backend/partitioning/partprune.c: 438 - 713

## Overview
Builds a list of PartitionedRelPruneInfo structures for each partitioned relation in a hierarchy, enabling runtime partition pruning during query execution.

## Definition
```c
static List *make_partitionedrel_pruneinfo(PlannerInfo *root, RelOptInfo *parentrel, List *prunequal, Bitmapset *partrelids, int *relid_subplan_map, Bitmapset **matchedsubplans)
```

## Detailed Description
This function constructs detailed pruning information for each partitioned relation within a single partitioning hierarchy. It performs a two-phase process:

**Phase 1**: Examines each partitioned relation to determine if runtime pruning is needed and constructs basic PartitionedRelPruneInfo structures. It translates pruning qualifications for each partition level and generates both initial (startup) and execution-time pruning steps using gen_partprune_steps.

**Phase 2**: If runtime pruning is required, builds the mapping structures needed by the executor:
- subplan_map: Maps partition indexes to subplan indexes for leaf partitions
- subpart_map: Maps partition indexes to PartitionedRelPruneInfo indexes for sub-partitioned relations
- relid_map: Maps partition indexes to relation OIDs

The function handles multi-level partitioning by properly translating qualifications between different levels using adjust_appendrel_attrs and adjust_appendrel_attrs_multilevel.

## Parameters / Member Variables
- `root`: PlannerInfo containing global planning state and relation information  
- `parentrel`: RelOptInfo associated with the appendpath being considered
- `prunequal`: List of potential pruning qualifications represented for parentrel
- `partrelids`: Bitmapset of RT indexes identifying relevant partitioned tables within the hierarchy
- `relid_subplan_map`: Array mapping child relation relids to subplan indexes
- `matchedsubplans`: Output parameter receiving the set of matched subplan indexes

## Dependencies
- Functions called/Symbols referenced:
  - [gen_partprune_steps](../g/gen_partprune_steps.md)
  - [find_base_rel](../f/find_base_rel.md)
  - [adjust_appendrel_attrs](../a/adjust_appendrel_attrs.md)
  - [adjust_appendrel_attrs_multilevel](../a/adjust_appendrel_attrs_multilevel.md)
  - [find_appinfos_by_relids](../f/find_appinfos_by_relids.md)
  - [get_partkey_exec_paramids](../g/get_partkey_exec_paramids.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [bms_equal](../b/bms_equal.md)
  - bms_is_empty
  - [bms_add_member](../b/bms_add_member.md)
  - planner_rt_fetch
- Called from (representative examples):
  - [make_partition_pruneinfo](make_partition_pruneinfo.md)

## Notes and Other Information
- Returns NIL if no useful runtime pruning steps can be generated
- Detects contradictory qualifications and disables runtime pruning if found
- Distinguishes between startup pruning (for mutable operators/expressions) and per-scan pruning (for exec parameters)
- Uses 1-based indexing for relid_subpart_map and converts to 0-based indexing for final maps
- Handles cases where parentrel and target partition may have different column orders in sub-partitioned tables
- Creates present_parts bitmapset to track which partitions are available (not already pruned)
- The function is static and only used within the partition pruning subsystem
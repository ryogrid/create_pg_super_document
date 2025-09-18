# build_simple_rel

## Location
src/backend/optimizer/util/relnode.c: 192 - 413

## Overview
Constructs a new RelOptInfo structure for a base relation or other member relation, initializing all necessary fields and handling inheritance hierarchies.

## Definition
```c
RelOptInfo *build_simple_rel(PlannerInfo *root, int relid, RelOptInfo *parent)
```

## Detailed Description
This function creates and initializes a RelOptInfo structure, which is the fundamental data structure used by the PostgreSQL query planner to represent relations. The function handles different types of relations (tables, subqueries, functions, CTEs, etc.) and properly sets up inheritance relationships when a parent is provided.

Key responsibilities include:
- Creating a new RelOptInfo node with appropriate default values
- Setting up relation type (base relation or other member relation)
- Configuring user permissions and access control
- Handling inheritance hierarchy propagation (lateral references, nulling relations)
- Initializing attribute ranges and arrays based on relation type
- Applying parent quals to child relations through apply_child_basequals
- Storing the completed RelOptInfo in the simple_rel_array

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state
- `relid`: Range table index (1-based) identifying the relation 
- `parent`: Parent RelOptInfo for inheritance hierarchies, NULL for base relations

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create RelOptInfo structure)
  - [bms_make_singleton](bms_make_singleton.md) (for creating relation ID bitmaps)
  - [create_empty_pathtarget](../c/create_empty_pathtarget.md) (for target list initialization)
  - [getRTEPermissionInfo](../g/getRTEPermissionInfo.md) (for permission handling)
  - [get_relation_info](../g/get_relation_info.md) (for table statistics and metadata)
  - [apply_child_basequals](../a/apply_child_basequals.md) (for constraint inheritance)
  - [mark_dummy_rel](../m/mark_dummy_rel.md) (when constraints prove relation is empty)
- Constants used:
  - RELOPT_BASEREL, RELOPT_OTHER_MEMBER_REL (relation optimization kinds)
  - RTE_RELATION, RTE_SUBQUERY, RTE_FUNCTION, etc. (range table entry types)
- Data structures used:
  - RelOptInfo, RangeTblEntry, RTEPermissionInfo, AppendRelInfo
- Called from (representative examples):
  - [add_base_rels_to_query](../a/add_base_rels_to_query.md) (src/backend/optimizer/plan/initsplan.c:165)
  - [query_planner](../q/query_planner.md) (src/backend/optimizer/plan/planmain.c:107)
  - [expand_inherited_rtentry](../e/expand_inherited_rtentry.md) (src/backend/optimizer/util/inherit.c:221)
  - [expand_partitioned_rtentry](../e/expand_partitioned_rtentry.md) (src/backend/optimizer/util/inherit.c:414)

## Notes and Other Information
- Includes comprehensive validation to prevent duplicate relation creation
- Handles complex inheritance scenarios including lateral references and nulling relations
- Different initialization paths for different RTE types (relations vs subqueries vs functions)
- The function can mark child relations as dummy if inherited constraints prove they will be empty
- Must store RelOptInfo in simple_rel_array before calling apply_child_basequals due to transformation dependencies
- Supports both regular inheritance and partitioned table hierarchies
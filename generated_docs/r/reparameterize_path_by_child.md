# reparameterize_path_by_child

## Location
src/backend/optimizer/util/pathnode.c: 4115 - 4122

## Overview
Translates a path parameterized by a parent relation to be parameterized by a given child relation, supporting partitionwise joins by adjusting expressions and subpaths to refer to the correct variable numbers.

## Definition


## Detailed Description
This function is a core component of PostgreSQL's partitionwise join optimization. When the optimizer needs to create a partitionwise join plan, it must translate paths that were originally parameterized by parent relations to work with specific child (partition) relations.

The function performs several key operations:
1. **Early exit optimization**: Returns the original path unchanged if it's not parameterized by the parent of the given relation
2. **Type-specific reparameterization**: Uses a switch statement to handle different path types (T_Path, T_IndexPath, T_BitmapHeapPath, etc.), each requiring specific attribute adjustments
3. **Expression translation**: Adjusts expressions to use the correct variable numbers (varnos) for the child relation using the ADJUST_CHILD_ATTRS macro
4. **Recursive processing**: Reparameterizes subpaths recursively using REPARAMETERIZE_CHILD_PATH and REPARAMETERIZE_CHILD_PATH_LIST macros
5. **Parameter info adjustment**: Creates or finds appropriate ParamPathInfo structures for the new parameterization
6. **Target adjustment**: Updates path targets when lateral references are involved

The function is designed to work only during create_plan() phase when final path choices have been made, as it can modify RangeTblEntries, RelOptInfos, and IndexOptInfos.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and configuration
- : The original path parameterized by the parent relation that needs reparameterization  
- : The child relation (partition) that should be the new parameterization target

## Dependencies
- Functions called/Symbols referenced:
  - ParamPathInfo (structure used for parameterization information)
  - adjust_appendrel_attrs_multilevel (via ADJUST_CHILD_ATTRS macro)
  - adjust_child_relids_multilevel
  - find_param_path_info
  - makeNode
  - copy_pathtarget
  - bms_overlap, bms_copy, bms_free (bitmap set operations)
- Called from (representative examples):
  - create_nestloop_plan (src/backend/optimizer/plan/createplan.c:4368)
  - reparameterize_pathlist_by_child (src/backend/optimizer/util/pathnode.c:4551)
  - REPARAMETERIZE_CHILD_PATH macro (self-referential for recursive calls)

## Notes and Other Information
- **Supported Path Types**: The function supports specific path types commonly used in partitionwise joins: T_Path, T_IndexPath, T_BitmapHeapPath, T_BitmapAndPath, T_BitmapOrPath, T_ForeignPath, T_CustomPath, T_NestPath, T_MergePath, T_HashPath, T_AppendPath, T_MaterialPath, T_MemoizePath, T_GatherPath
- **Unsupported Types**: Returns NULL for unsupported path types, which prevents partitionwise join generation for those cases
- **Synchronization Requirement**: Must be kept in sync with path_is_reparameterizable_by_child() function
- **Memory Context**: Allocates new ParamPathInfo in the same memory context as the parent RelOptInfo
- **Safety**: Only safe to call during create_plan() when final path decisions have been made
- **Cost Preservation**: Path costs, row counts, width, and parallel properties are preserved since they depend on path->parent which doesn't change
- **FDW Integration**: Provides hooks for Foreign Data Wrappers to handle custom reparameterization via ReparameterizeForeignPathByChild_function
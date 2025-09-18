# apply_scanjoin_target_to_paths

## Location
src/backend/optimizer/plan/planner.c: 7705 - 7939

## Overview
Recursively adjusts the final scan/join relation and all its children to generate the target output by either updating sortgroupref information or creating projection paths, handling partitioned relations and parallel safety constraints.

## Definition


## Detailed Description
This function is a critical component of PostgreSQL's query planning that transforms scan/join relations to produce the correct final output. It operates through several sophisticated mechanisms:

1. **Target list optimization**: When tlist_same_exprs is true, efficiently updates sortgroupref information without creating new paths
2. **Projection path creation**: For different expressions, wraps existing paths with projection paths to generate the required target
3. **Partitioned relation handling**: For partitioned tables, recursively processes all live partitions and generates new Append paths with computation below the Append node
4. **Parallel safety management**: Handles non-parallel-safe targets by generating Gather paths before applying targets and disabling further parallelism
5. **SRF processing**: Integrates Set-Returning Functions by adding ProjectSetPath nodes when the target contains SRFs
6. **Cost-based path management**: Ensures optimal path selection through set_cheapest after all transformations

The function balances correctness, performance, and parallelism while maintaining plan consistency across platforms.

## Parameters / Member Variables
- : PlannerInfo containing query planning context and metadata
- : RelOptInfo representing the relation whose paths need target adjustment
- : List of PathTarget objects representing different target list variants
- : List indicating which targets contain Set-Returning Functions
- : Boolean flag indicating whether the target can be computed in parallel workers
- : Boolean optimization flag - when true, only sortgroupref information needs updating

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth
  - [generate_useful_gather_paths](../g/generate_useful_gather_paths.md)  
  - [create_projection_path](../c/create_projection_path.md)
  - [adjust_paths_for_srfs](adjust_paths_for_srfs.md)
  - [find_appinfos_by_relids](../f/find_appinfos_by_relids.md)
  - [copy_pathtarget](../c/copy_pathtarget.md)
  - [adjust_appendrel_attrs](adjust_appendrel_attrs.md)
  - [add_paths_to_append_rel](add_paths_to_append_rel.md)
  - [set_cheapest](../s/set_cheapest.md)
  - IS_PARTITIONED_REL
  - IS_DUMMY_REL
  - IS_OTHER_REL
- Called from (representative examples):
  - [grouping_planner](../g/grouping_planner.md)
  - [apply_scanjoin_target_to_paths](apply_scanjoin_target_to_paths.md) (recursive)
  - standard_qp_extra

## Notes and Other Information
- Implements recursive processing with stack depth checking for deep partition hierarchies
- For partitioned relations, drops existing paths and forces computation below Append nodes for better cost consistency
- Handles parallel vs non-parallel target transitions by strategically placing Gather operations
- Updates rel->reltarget to match actual path outputs, ensuring consistency for createplan.c and FDW calls
- Critical for partitionwise aggregate optimization by computing targets at partition level
- The function modifies path lists in-place for efficiency while maintaining path ordering
- Location: src/backend/optimizer/plan/planner.c:7705-7939
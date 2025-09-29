# apply_scanjoin_target_to_paths

## Location
[src/backend/optimizer/plan/planner.c:7705-7939](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L7705-L7939)

## Overview
Recursively adjusts the final scan/join relation and all its children to generate the target output by either updating sortgroupref information or creating projection paths, handling partitioned relations and parallel safety constraints.

## Definition

```c
static void
apply_scanjoin_target_to_paths(PlannerInfo *root,
							   RelOptInfo *rel,
							   List *scanjoin_targets,
							   List *scanjoin_targets_contain_srfs,
							   bool scanjoin_target_parallel_safe,
							   bool tlist_same_exprs)
```
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
  - [check_stack_depth](../c/check_stack_depth.md)
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

## Simplified Source

```c
static void
apply_scanjoin_target_to_paths(PlannerInfo *root,
                               RelOptInfo *rel,
                               List *scanjoin_targets,
                               List *scanjoin_targets_contain_srfs,
                               bool scanjoin_target_parallel_safe,
                               bool tlist_same_exprs)
{
    bool rel_is_partitioned = IS_PARTITIONED_REL(rel);
    PathTarget *scanjoin_target;
    ListCell *lc;

    check_stack_depth();

    // For partitioned relations, drop existing paths
    if (rel_is_partitioned)
        rel->pathlist = NIL;

    // Handle non-parallel-safe targets
    if (!scanjoin_target_parallel_safe)
    {
        // Generate Gather paths before losing parallel capability
        generate_useful_gather_paths(root, rel, false);
        rel->partial_pathlist = NIL;
        rel->consider_parallel = false;
    }

    if (rel_is_partitioned)
        rel->partial_pathlist = NIL;

    // Get the SRF-free scan/join target
    scanjoin_target = linitial_node(PathTarget, scanjoin_targets);

    // Apply target to existing paths
    foreach(lc, rel->pathlist)
    {
        Path *subpath = (Path *) lfirst(lc);
        Assert(subpath->param_info == NULL);

        if (tlist_same_exprs)
            // Just update sortgroupref info
            subpath->pathtarget->sortgrouprefs = scanjoin_target->sortgrouprefs;
        else
        {
            // Create projection path
            Path *newpath = (Path *) create_projection_path(root, rel, subpath, scanjoin_target);
            lfirst(lc) = newpath;
        }
    }

    // Handle partial paths similarly
    foreach(lc, rel->partial_pathlist)
    {
        Path *subpath = (Path *) lfirst(lc);
        Assert(subpath->param_info == NULL);

        if (tlist_same_exprs)
            subpath->pathtarget->sortgrouprefs = scanjoin_target->sortgrouprefs;
        else
        {
            Path *newpath = (Path *) create_projection_path(root, rel, subpath, scanjoin_target);
            lfirst(lc) = newpath;
        }
    }

    // Handle SRFs if present
    if (root->parse->hasTargetSRFs)
        adjust_paths_for_srfs(root, rel, scanjoin_targets, scanjoin_targets_contain_srfs);

    // Update relation target
    rel->reltarget = llast_node(PathTarget, scanjoin_targets);

    // Handle partitioned relations recursively
    if (rel_is_partitioned)
    {
        List *live_children = NIL;
        int i = -1;

        // Process each partition
        while ((i = bms_next_member(rel->live_parts, i)) >= 0)
        {
            RelOptInfo *child_rel = rel->part_rels[i];
            AppendRelInfo **appinfos;
            int nappinfos;
            List *child_scanjoin_targets = NIL;

            if (!child_rel || IS_DUMMY_REL(child_rel))
                continue;

            // Translate targets for this partition
            appinfos = find_appinfos_by_relids(root, child_rel->relids, &nappinfos);
            foreach(lc, scanjoin_targets)
            {
                PathTarget *target = lfirst_node(PathTarget, lc);
                target = copy_pathtarget(target);
                target->exprs = (List *) adjust_appendrel_attrs(root,
                                                               (Node *) target->exprs,
                                                               nappinfos, appinfos);
                child_scanjoin_targets = lappend(child_scanjoin_targets, target);
            }
            pfree(appinfos);

            // Recursive call for child
            apply_scanjoin_target_to_paths(root, child_rel,
                                          child_scanjoin_targets,
                                          scanjoin_targets_contain_srfs,
                                          scanjoin_target_parallel_safe,
                                          tlist_same_exprs);

            if (!IS_DUMMY_REL(child_rel))
                live_children = lappend(live_children, child_rel);
        }

        // Build new Append paths
        add_paths_to_append_rel(root, rel, live_children);
    }

    // Generate Gather paths if appropriate
    if (rel->consider_parallel && !IS_OTHER_REL(rel))
        generate_useful_gather_paths(root, rel, false);

    // Update cheapest paths
    set_cheapest(rel);
}
```
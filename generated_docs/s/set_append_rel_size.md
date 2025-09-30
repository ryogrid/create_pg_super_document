# set_append_rel_size

## Location
[src/backend/optimizer/path/allpaths.c:944-1231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L944-L1231)

## Overview
Sets size estimates for an append relation by computing aggregate statistics across all child relations, handling inheritance trees and partitioned tables.

## Definition

```c
structures as well.  This is needed either if the parent
		 * participates in some eclass joins (because we will want to consider
		 * inner-indexscan joins on the individual children) or if the parent
		 * has useful pathkeys (because we should try to build MergeAppend
		 * paths that produce those sort orderings).
		 */
		if (rel->has_eclass_joins || has_useful_pathkeys(root, rel))
			add_child_rel_equivalences(root, appinfo, rel, childrel);
```
## Detailed Description
This function computes size estimates for append relations, which represent tables accessed through inheritance hierarchies or partitioned tables. It processes each child relation in the append relation list, applies constraint exclusion to eliminate children that cannot produce rows, and aggregates size statistics from all remaining live children.

The function handles several key responsibilities: it sets up partitionwise join consideration for partitioned base relations, copies the parent's targetlist and join quals to each child with appropriate variable substitutions, manages equivalence class relationships for join optimization, determines parallel safety across all children, and computes weighted size estimates based on each child's contribution.

Width estimates are computed by weighting child relation widths proportionally to their row counts, which provides accurate footprint estimates for operations like sorting or hashing. The function also handles constraint exclusion at the child level and can mark children as dummy if they're proven to produce no rows.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner information and append relation list
- : RelOptInfo structure for the append relation that will be updated with aggregated size estimates
- : Range table index of the parent relation in the append hierarchy
- : RangeTblEntry for the parent relation containing relation metadata

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md) (prevents stack overflow in deep inheritance trees)
  - IS_SIMPLE_REL (checks if relation is a simple base relation)
  - bms_is_empty (checks if bitmap set is empty)
  - [find_base_rel](../f/find_base_rel.md) (locates child relation's RelOptInfo)
  - IS_DUMMY_REL (checks if relation produces no rows)
  - [relation_excluded_by_constraints](../r/relation_excluded_by_constraints.md) (applies constraint exclusion)
  - [set_dummy_rel_pathlist](set_dummy_rel_pathlist.md) (marks relation as producing no rows)
  - [bms_overlap](../b/bms_overlap.md) (checks bitmap set overlap for nulling relations)
  - [adjust_appendrel_attrs](../a/adjust_appendrel_attrs.md) (translates expressions between parent and child)
  - [has_useful_pathkeys](../h/has_useful_pathkeys.md) (checks if relation has useful sort orders)
  - [add_child_rel_equivalences](../a/add_child_rel_equivalences.md) (creates equivalence class entries)
  - [set_rel_consider_parallel](set_rel_consider_parallel.md) (determines parallel safety)
  - [set_rel_size](set_rel_size.md) (recursively computes child relation sizes)
  - forboth (macro for parallel iteration over two lists)
  - [get_typavgwidth](../g/get_typavgwidth.md) (gets average width for a data type)
  - [exprType](../e/exprType.md)/exprTypmod (extracts type information from expressions)

- Called from (representative examples):
  - [set_rel_size](set_rel_size.md) (main relation sizing dispatcher)

## Notes and Other Information
- This function is static and only used within allpaths.c
- Guards against stack overflow in deeply nested inheritance hierarchies
- Supports both traditional inheritance and partitioned table hierarchies
- Implements constraint exclusion to eliminate impossible child relations
- Handles partitionwise join setup for partitioned base relations
- Manages parallel safety propagation - if any child is not parallel-safe, the whole append relation is marked unsafe
- Width estimation uses row-weighted averaging across all live children
- Leaves rel->pages as zero to avoid double-counting in total_table_pages
- Can result in a dummy append relation if all children are excluded by constraints
- The function assumes child RelOptInfo structures have already been created during add_other_rels_to_query
- [Variable](../V/Variable.md) substitution between parent and child uses the AppendRelInfo translation mappings

## Simplified Source

```c
static void
set_append_rel_size(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
    int parentRTindex = rti;
    bool has_live_children = false;
    double parent_rows = 0;
    double parent_size = 0;
    double *parent_attrsizes;
    int nattrs;
    ListCell *l;

    // Guard against stack overflow in deep inheritance trees
    check_stack_depth();

    // Enable partitionwise joins for partitioned base relations
    if (enable_partitionwise_join &&
        rel->reloptkind == RELOPT_BASEREL &&
        rte->relkind == RELKIND_PARTITIONED_TABLE &&
        bms_is_empty(rel->attr_needed[InvalidAttrNumber - rel->min_attr]))
        rel->consider_partitionwise_join = true;

    // Initialize size estimation variables
    nattrs = rel->max_attr - rel->min_attr + 1;
    parent_attrsizes = (double *) palloc0(nattrs * sizeof(double));

    // Process each child relation in the append relation list
    foreach(l, root->append_rel_list)
    {
        AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);
        int childRTindex;
        RangeTblEntry *childRTE;
        RelOptInfo *childrel;
        List *childrinfos;
        ListCell *parentvars, *childvars;

        // Skip if not a child of this parent
        if (appinfo->parent_relid != parentRTindex)
            continue;

        childRTindex = appinfo->child_relid;
        childRTE = root->simple_rte_array[childRTindex];
        childrel = find_base_rel(root, childRTindex);

        // Skip dummy relations
        if (IS_DUMMY_REL(childrel))
            continue;

        // Apply constraint exclusion
        if (relation_excluded_by_constraints(root, childrel, childRTE))
        {
            set_dummy_rel_pathlist(childrel);
            continue;
        }

        // Copy parent's join quals to child (excluding nulling joins)
        childrinfos = NIL;
        foreach(lc, rel->joininfo)
        {
            RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
            if (!bms_overlap(rinfo->clause_relids, rel->nulling_relids))
                childrinfos = lappend(childrinfos,
                    adjust_appendrel_attrs(root, (Node *) rinfo, 1, &appinfo));
        }
        childrel->joininfo = childrinfos;

        // Copy parent's targetlist to child with variable substitution
        childrel->reltarget->exprs = (List *)
            adjust_appendrel_attrs(root, (Node *) rel->reltarget->exprs, 1, &appinfo);

        // Set up equivalence classes for joins and pathkeys
        if (rel->has_eclass_joins || has_useful_pathkeys(root, rel))
            add_child_rel_equivalences(root, appinfo, rel, childrel);
        childrel->has_eclass_joins = rel->has_eclass_joins;

        // Propagate partitionwise join setting
        if (rel->consider_partitionwise_join)
            childrel->consider_partitionwise_join = true;

        // Check parallel safety
        if (root->glob->parallelModeOK && rel->consider_parallel)
            set_rel_consider_parallel(root, childrel, childRTE);

        // Compute child's size estimates
        set_rel_size(root, childrel, childRTindex, childRTE);

        // Skip if child became dummy after sizing
        if (IS_DUMMY_REL(childrel))
            continue;

        has_live_children = true;

        // Disable parallel processing if any child is not parallel-safe
        if (!childrel->consider_parallel)
            rel->consider_parallel = false;

        // Accumulate size statistics from this child
        parent_rows += childrel->rows;
        parent_size += childrel->reltarget->width * childrel->rows;

        // Accumulate per-column width estimates
        forboth(parentvars, rel->reltarget->exprs, childvars, childrel->reltarget->exprs)
        {
            Var *parentvar = (Var *) lfirst(parentvars);
            Node *childvar = (Node *) lfirst(childvars);

            if (IsA(parentvar, Var) && parentvar->varno == parentRTindex)
            {
                int pndx = parentvar->varattno - rel->min_attr;
                int32 child_width = 0;

                // Get width from child if it's a Var, otherwise use type default
                if (IsA(childvar, Var) && ((Var *) childvar)->varno == childrel->relid)
                {
                    int cndx = ((Var *) childvar)->varattno - childrel->min_attr;
                    child_width = childrel->attr_widths[cndx];
                }
                if (child_width <= 0)
                    child_width = get_typavgwidth(exprType(childvar), exprTypmod(childvar));

                parent_attrsizes[pndx] += child_width * childrel->rows;
            }
        }
    }

    if (has_live_children)
    {
        // Set final size estimates using weighted averages
        rel->rows = parent_rows;
        rel->reltarget->width = rint(parent_size / parent_rows);
        for (int i = 0; i < nattrs; i++)
            rel->attr_widths[i] = rint(parent_attrsizes[i] / parent_rows);
        rel->tuples = parent_rows;
        // Leave rel->pages = 0 to avoid double-counting
    }
    else
    {
        // All children excluded - mark append relation as dummy
        set_dummy_rel_pathlist(rel);
    }

    pfree(parent_attrsizes);
}
```
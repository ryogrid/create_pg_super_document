# split_pathtarget_at_srfs

## Location
[src/backend/optimizer/util/tlist.c:881-1076](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L881-L1076)

## Overview
Splits a given PathTarget into multiple levels to position set-returning functions (SRFs) safely, ensuring each level satisfies the executor's constraint that SRFs can only appear at the top level of a ProjectSet plan node.

## Definition
void split_pathtarget_at_srfs(PlannerInfo *root, PathTarget *target, PathTarget *input_target, List **targets, List **targets_contain_srfs)

## Detailed Description
The PostgreSQL executor can only handle set-returning functions that appear at the top level of the targetlist of a ProjectSet plan node. When SRFs are nested within expressions or appear at non-top levels, the evaluation must be split into multiple plan levels where each level satisfies this constraint.

This function analyzes a PathTarget containing potentially nested SRFs and creates a hierarchy of PathTargets representing the evaluation levels needed. For example, the expression 'x + srf1(srf2(y + z))' would be split into:
- Level 0 (bottom): x, y, z (no SRFs)
- Level 1: x, srf2(y + z)
- Level 2: x, srf1(srf2(y + z))
- Level 3 (top): x + srf1(srf2(y + z))

The function preserves sortgroupref annotations and handles cases where SRFs have already been evaluated in previous plan levels (indicated by input_target). It returns two parallel lists: PathTargets for each level and boolean flags indicating whether each level contains evaluable SRFs.

## Parameters / Member Variables
- root: PlannerInfo structure containing planner context
- target: The original PathTarget that needs to be split
- input_target: PathTarget representing expressions already available from input (can be NULL)
- targets: Output parameter returning list of PathTargets for each evaluation level
- targets_contain_srfs: Output parameter returning list of boolean flags indicating SRF presence

## Dependencies
- Functions called/Symbols referenced:
  - [split_pathtarget_walker](split_pathtarget_walker.md) (walks expressions to find and categorize SRFs and Vars)
  - get_pathtarget_sortgroupref (retrieves sortgroupref for expressions)
  - [create_empty_pathtarget](../c/create_empty_pathtarget.md) (creates new empty PathTarget structures)
  - [add_sp_items_to_pathtarget](../a/add_sp_items_to_pathtarget.md), add_sp_item_to_pathtarget (adds items to PathTargets)
  - [set_pathtarget_cost_width](set_pathtarget_cost_width.md) (calculates cost and width estimates)
  - IS_SRF_CALL (macro to check if node is an SRF call)
  - Various list manipulation functions (list_make1, list_concat, lappend, etc.)
- Called from (representative examples):
  - [grouping_planner](../g/grouping_planner.md) (in src/backend/optimizer/plan/planner.c:1634, 1640, 1646, 1652)

## Notes and Other Information
- The function uses a sophisticated algorithm to track SRF nesting depth and organize expressions into appropriate evaluation levels
- Preserves sortgroupref annotations which are crucial for ORDER BY and GROUP BY operations
- Handles optimization cases like identical input/target PathTargets and SRF-free expressions
- The output lists are ordered from lowest (most basic) to highest (original target) evaluation level
- Uses helper structures like split_pathtarget_context and split_pathtarget_item for internal organization
- Critical for proper execution of queries with complex SRF expressions that cannot be evaluated in a single ProjectSet node

## Simplified Source

```c
void split_pathtarget_at_srfs(PlannerInfo *root,
                             PathTarget *target, PathTarget *input_target,
                             List **targets, List **targets_contain_srfs)
{
    split_pathtarget_context context;
    int max_depth;
    bool need_extra_projection;
    List *prev_level_tlist;
    int lci;

    // Quick optimization: if target equals input_target, no splitting needed
    if (target == input_target) {
        *targets = list_make1(target);
        *targets_contain_srfs = list_make1_int(false);
        return;
    }

    // Initialize context for SRF analysis
    context.input_target_exprs = input_target ? input_target->exprs : NIL;
    context.level_srfs = list_make1(NIL);
    context.level_input_vars = list_make1(NIL);
    context.level_input_srfs = list_make1(NIL);
    context.current_input_vars = NIL;
    context.current_input_srfs = NIL;
    max_depth = 0;
    need_extra_projection = false;

    // Analyze each expression in the PathTarget for SRFs
    lci = 0;
    foreach(lc, target->exprs) {
        Node *node = (Node *) lfirst(lc);

        context.current_sgref = get_pathtarget_sortgroupref(target, lci);
        lci++;

        // Find SRFs and Vars in this expression
        context.current_depth = 0;
        split_pathtarget_walker(node, &context);

        // Skip expressions with no SRFs
        if (context.current_depth == 0)
            continue;

        // Track maximum SRF nesting depth
        if (max_depth < context.current_depth) {
            max_depth = context.current_depth;
            need_extra_projection = false;
        }

        // Check if extra projection level is needed
        if (max_depth == context.current_depth && !IS_SRF_CALL(node))
            need_extra_projection = true;
    }

    // No SRFs found that need evaluation
    if (max_depth == 0) {
        *targets = list_make1(target);
        *targets_contain_srfs = list_make1_int(false);
        return;
    }

    // Add top-level variables to appropriate level
    if (need_extra_projection) {
        context.level_srfs = lappend(context.level_srfs, NIL);
        context.level_input_vars = lappend(context.level_input_vars,
                                          context.current_input_vars);
        context.level_input_srfs = lappend(context.level_input_srfs,
                                          context.current_input_srfs);
    } else {
        // Add to existing max_depth level
        ListCell *lc = list_nth_cell(context.level_input_vars, max_depth);
        lfirst(lc) = list_concat(lfirst(lc), context.current_input_vars);
        lc = list_nth_cell(context.level_input_srfs, max_depth);
        lfirst(lc) = list_concat(lfirst(lc), context.current_input_srfs);
    }

    // Construct output PathTargets for each level
    *targets = *targets_contain_srfs = NIL;
    prev_level_tlist = NIL;

    forthree(lc1, context.level_srfs,
             lc2, context.level_input_vars,
             lc3, context.level_input_srfs) {
        List *level_srfs = (List *) lfirst(lc1);
        PathTarget *ntarget;

        if (lnext(context.level_srfs, lc1) == NULL) {
            // Use original target for final level
            ntarget = target;
        } else {
            // Create new target for intermediate level
            ntarget = create_empty_pathtarget();

            // Add SRFs for this level
            add_sp_items_to_pathtarget(ntarget, level_srfs);

            // Add input vars needed by later levels
            for_each_cell(lc, context.level_input_vars,
                         lnext(context.level_input_vars, lc2)) {
                List *input_vars = (List *) lfirst(lc);
                add_sp_items_to_pathtarget(ntarget, input_vars);
            }

            // Add input SRFs from earlier levels
            for_each_cell(lc, context.level_input_srfs,
                         lnext(context.level_input_srfs, lc3)) {
                List *input_srfs = (List *) lfirst(lc);
                foreach(lcx, input_srfs) {
                    split_pathtarget_item *item = lfirst(lcx);
                    if (list_member(prev_level_tlist, item->expr))
                        add_sp_item_to_pathtarget(ntarget, item);
                }
            }

            set_pathtarget_cost_width(root, ntarget);
        }

        // Add to output lists
        *targets = lappend(*targets, ntarget);
        *targets_contain_srfs = lappend_int(*targets_contain_srfs,
                                           (level_srfs != NIL));

        prev_level_tlist = ntarget->exprs;
    }
}
```
# recurse_set_operations

## Location
[src/backend/optimizer/prep/prepunion.c:230-383](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepunion.c#L230-L383)

## Overview
Recursively processes set operation trees, handling each node to generate optimized execution paths and appropriate target lists for UNION, INTERSECT, and EXCEPT operations.

## Definition

```c
union_paths(op, root,
									   refnames_tlist,
									   pTargetList);
```
## Detailed Description
 is the core recursive function that traverses and processes set operation trees in PostgreSQL's query optimizer. It handles two main types of nodes: leaf nodes (RangeTblRef) representing base subqueries, and internal nodes (SetOperationStmt) representing set operations.

For leaf nodes (subqueries):
- Builds a RelOptInfo for the subquery
- Invokes the subquery planner to generate execution paths 
- Creates appropriate target lists with proper column types and collations
- Handles flag columns for set operation processing

For internal nodes (set operations):
- Delegates to specialized functions based on operation type (UNION vs INTERSECT/EXCEPT)
- Calls  for UNION operations
- Calls  for INTERSECT/EXCEPT operations
- Applies projections when necessary to ensure proper column types and collations
- Handles both regular and partial (parallel) execution paths

The function manages target list generation carefully, ensuring that column names, types, and collations match the expected output schema. It also handles flag columns used internally for duplicate elimination and set operation logic.

## Parameters / Member Variables
- : Node representing either a RangeTblRef (leaf subquery) or SetOperationStmt (set operation)
- : PlannerInfo containing the overall query planning context
- : List of OIDs specifying the expected result column data types
- : List of OIDs specifying the expected result column collations
- : Boolean indicating whether child resjunk columns may be left in the result
- : Integer flag value; if >= 0, adds a resjunk output column with this value
- : Target list providing column names for the result
- : Output parameter receiving the fully-fledged target list for the subtree's top plan
- : Output parameter indicating whether datatypes between parent and child match exactly

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md)
  - [build_simple_rel](../b/build_simple_rel.md)
  - [subquery_planner](../s/subquery_planner.md)
  - [generate_setop_tlist](../g/generate_setop_tlist.md)
  - create_pathtarget
  - [generate_union_paths](../g/generate_union_paths.md)
  - [generate_nonunion_paths](../g/generate_nonunion_paths.md)
  - [tlist_same_datatypes](../t/tlist_same_datatypes.md)
  - [tlist_same_collations](../t/tlist_same_collations.md)
  - [apply_projection_to_path](../a/apply_projection_to_path.md)
  - [create_projection_path](../c/create_projection_path.md)
  - [postprocess_setop_rel](../p/postprocess_setop_rel.md)
- Called from (representative examples):
  - [plan_set_operations](../p/plan_set_operations.md) (src/backend/optimizer/prep/prepunion.c:166)
  - [generate_recursion_path](../g/generate_recursion_path.md) (src/backend/optimizer/prep/prepunion.c:412, 424)
  - [generate_nonunion_paths](../g/generate_nonunion_paths.md) (src/backend/optimizer/prep/prepunion.c:1051, 1064)
  - [plan_union_children](../p/plan_union_children.md) (src/backend/optimizer/prep/prepunion.c:1254)

## Notes and Other Information
- This is a static function, internal to the prepunion.c module
- The function includes stack depth checking to prevent overflow from overly complex set operation nests
- Type modifiers (typmods) are not a concern here since the only allowed difference is specific typmod to -1, requiring no coercion
- The function carefully manages both regular and partial (parallel) execution paths
- Target list handling is complex due to the need to maintain proper column ordering and types while handling resjunk flag columns
- The pTargetList output parameter is somewhat redundant with the RelOptInfo's pathtarget but is needed for proper flag column handling
- Cross-references between subqueries in the setop tree are not allowed and will trigger an error

## Simplified Source

```c
static RelOptInfo *recurse_set_operations(Node *setOp, PlannerInfo *root,
                                        List *colTypes, List *colCollations,
                                        bool junkOK, int flag, List *refnames_tlist,
                                        List **pTargetList, bool *istrivial_tlist) {
    RelOptInfo *rel;

    *istrivial_tlist = true;

    // Prevent stack overflow from deeply nested setops
    check_stack_depth();

    if (IsA(setOp, RangeTblRef)) {
        // Handle leaf node (subquery)
        RangeTblRef *rtr = (RangeTblRef *) setOp;
        RangeTblEntry *rte = root->simple_rte_array[rtr->rtindex];
        SetOperationStmt *setops;
        Query *subquery = rte->subquery;
        PlannerInfo *subroot;
        List *tlist;
        bool trivial_tlist;

        Assert(subquery != NULL);

        // Build RelOptInfo for this subquery
        rel = build_simple_rel(root, rtr->rtindex, NULL);
        Assert(root->plan_params == NIL);

        // Plan the subquery
        setops = castNode(SetOperationStmt, root->parse->setOperations);
        subroot = rel->subroot = subquery_planner(root->glob, subquery, root,
                                                false, root->tuple_fraction, setops);

        // Check for unexpected cross-references
        if (root->plan_params)
            elog(ERROR, "unexpected outer reference in set operation subquery");

        // Generate target list for subquery
        tlist = generate_setop_tlist(colTypes, colCollations, flag, rtr->rtindex,
                                   true, subroot->processed_tlist,
                                   refnames_tlist, &trivial_tlist);
        rel->reltarget = create_pathtarget(root, tlist);

        *pTargetList = tlist;
        *istrivial_tlist = trivial_tlist;

    } else if (IsA(setOp, SetOperationStmt)) {
        // Handle internal node (set operation)
        SetOperationStmt *op = (SetOperationStmt *) setOp;

        // Delegate to operation-specific functions
        if (op->op == SETOP_UNION) {
            rel = generate_union_paths(op, root, refnames_tlist, pTargetList);
        } else {
            rel = generate_nonunion_paths(op, root, refnames_tlist, pTargetList);
        }

        // Apply projection if needed to match expected types/collations
        if (flag >= 0 ||
            !tlist_same_datatypes(*pTargetList, colTypes, junkOK) ||
            !tlist_same_collations(*pTargetList, colCollations, junkOK)) {

            PathTarget *target;
            bool trivial_tlist;
            ListCell *lc;

            // Generate new target list with proper types
            *pTargetList = generate_setop_tlist(colTypes, colCollations, flag, 0,
                                              false, *pTargetList, refnames_tlist,
                                              &trivial_tlist);
            *istrivial_tlist = trivial_tlist;
            target = create_pathtarget(root, *pTargetList);

            // Apply projection to regular paths
            foreach(lc, rel->pathlist) {
                Path *subpath = (Path *) lfirst(lc);
                Path *path;

                Assert(subpath->param_info == NULL);
                path = apply_projection_to_path(root, subpath->parent, subpath, target);
                if (path != subpath)
                    lfirst(lc) = path;
            }

            // Apply projection to partial paths
            foreach(lc, rel->partial_pathlist) {
                Path *subpath = (Path *) lfirst(lc);
                Path *path;

                Assert(subpath->param_info == NULL);
                path = (Path *) create_projection_path(root, subpath->parent,
                                                     subpath, target);
                lfirst(lc) = path;
            }
        }
        postprocess_setop_rel(root, rel);

    } else {
        // Unknown node type
        elog(ERROR, "unrecognized node type: %d", (int) nodeTag(setOp));
        *pTargetList = NIL;
        rel = NULL;
    }

    return rel;
}
```
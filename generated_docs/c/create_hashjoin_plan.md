# create_hashjoin_plan

## Location
[src/backend/optimizer/plan/createplan.c:4747-4935](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L4747-L4935)

## Overview
Creates a HashJoin plan node from a HashPath, implementing hash joins where the inner relation is used to build a hash table that is then probed by the outer relation.

## Definition

```c
structed into outer/inner expressions, so they can be computed
	 * separately (inner expressions are used to build the hashtable via Hash,
	 * outer expressions to perform lookups of tuples from HashJoin's outer
	 * plan in the hashtable). Also collect operator information necessary to
	 * build the hashtable.
	 */
	foreach(lc, hashclauses)
	{
		OpExpr	   *hclause = lfirst_node(OpExpr, lc);

		hashoperators = lappend_oid(hashoperators, hclause->opno);
		hashcollations = lappend_oid(hashcollations, hclause->inputcollid);
		outer_hashkeys = lappend(outer_hashkeys, linitial(hclause->args));
		inner_hashkeys = lappend(inner_hashkeys, lsecond(hclause->args));
	}

	/*
	 * Build the hash node and hash join node.
	 */
	hash_plan = make_hash(inner_plan,
						  inner_hashkeys,
						  skewTable,
						  skewColumn,
						  skewInherit);
```
## Detailed Description
This function creates a HashJoin execution plan node from a HashPath. Hash joins are efficient when one relation (typically the smaller inner relation) can fit in a hash table built in memory, which is then probed by the outer relation to find matches. The function creates both a Hash node for building the hash table and a HashJoin node for the actual join operation. It handles hash key extraction from join clauses, sets up skew optimization for single-column joins when statistics are available, and manages batching for large datasets that don't fit in memory. The function also handles parallel execution by setting up shared hash table sizing information.

## Parameters

- `root`: PlannerInfo structure containing global planning context and state information
- `best_path`: HashPath representing the chosen hash join access path with batching and hash clause information

## Dependencies
- Functions called/Symbols referenced:
  - [build_path_tlist](../b/build_path_tlist.md)
  - [create_plan_recurse](create_plan_recurse.md)
  - [order_qual_clauses](../o/order_qual_clauses.md)
  - IS_OUTER_JOIN
  - [extract_actual_join_clauses](../e/extract_actual_join_clauses.md)
  - [extract_actual_clauses](../e/extract_actual_clauses.md)
  - [get_actual_clauses](../g/get_actual_clauses.md)
  - [list_difference](../l/list_difference.md)
  - [replace_nestloop_params](../r/replace_nestloop_params.md)
  - [get_switched_clauses](../g/get_switched_clauses.md)
  - [is_opclause](../i/is_opclause.md)
  - [lappend_oid](../l/lappend_oid.md)
  - lsecond
  - [make_hash](../m/make_hash.md)
  - [copy_plan_costsize](copy_plan_costsize.md)
  - [make_hashjoin](../m/make_hashjoin.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
- Called from (representative examples):
  - [create_join_plan](create_join_plan.md)

## Notes and Other Information
- [Hash](../H/Hash.md) joins are typically the most efficient join method when one relation is much smaller than the other
- Creates separate Hash and HashJoin nodes - the Hash node builds the hash table from the inner relation
- Implements skew optimization for single-column joins when column statistics indicate data skew
- Handles batching for large datasets that exceed work_mem by spilling to disk
- Supports parallel execution with shared hash tables across multiple workers
- Extracts hash keys and operators needed for the hash table implementation
- Requests small target lists from inputs to minimize memory usage during hash table operations
- Located at src/backend/optimizer/plan/createplan.c:4747-4935
- Part of the JOIN METHODS section of the planner

## Simplified Source

```c
static HashJoin *create_hashjoin_plan(PlannerInfo *root, HashPath *best_path) {
    List *tlist = build_path_tlist(root, &best_path->jpath.path);

    // Create input plans with appropriate target list sizes
    Plan *outer_plan = create_plan_recurse(root, best_path->jpath.outerjoinpath,
                                          (best_path->num_batches > 1) ? CP_SMALL_TLIST : 0);
    Plan *inner_plan = create_plan_recurse(root, best_path->jpath.innerjoinpath, CP_SMALL_TLIST);

    // Process join clauses
    List *joinclauses = order_qual_clauses(root, best_path->jpath.joinrestrictinfo);
    List *otherclauses = NIL;

    if (IS_OUTER_JOIN(best_path->jpath.jointype)) {
        extract_actual_join_clauses(joinclauses, best_path->jpath.path.parent->relids,
                                   &joinclauses, &otherclauses);
    } else {
        joinclauses = extract_actual_clauses(joinclauses, false);
    }

    // Extract hash clauses and remove from join clauses
    List *hashclauses = get_actual_clauses(best_path->path_hashclauses);
    joinclauses = list_difference(joinclauses, hashclauses);

    // Handle nested loop parameters if needed
    if (best_path->jpath.path.param_info) {
        joinclauses = (List *) replace_nestloop_params(root, (Node *) joinclauses);
        otherclauses = (List *) replace_nestloop_params(root, (Node *) otherclauses);
    }

    // Arrange hash clauses with outer variable on left
    hashclauses = get_switched_clauses(best_path->path_hashclauses,
                                      best_path->jpath.outerjoinpath->parent->relids);

    // Collect skew optimization info for single hash clause
    Oid skewTable = InvalidOid;
    AttrNumber skewColumn = InvalidAttrNumber;
    bool skewInherit = false;

    if (list_length(hashclauses) == 1) {
        // Extract skew optimization parameters from single join clause
        OpExpr *clause = (OpExpr *) linitial(hashclauses);
        Node *node = (Node *) linitial(clause->args);
        if (IsA(node, RelabelType))
            node = (Node *) ((RelabelType *) node)->arg;
        if (IsA(node, Var)) {
            Var *var = (Var *) node;
            RangeTblEntry *rte = root->simple_rte_array[var->varno];
            if (rte->rtekind == RTE_RELATION) {
                skewTable = rte->relid;
                skewColumn = var->varattno;
                skewInherit = rte->inh;
            }
        }
    }

    // Build hash key lists and operators
    List *hashoperators = NIL, *hashcollations = NIL;
    List *inner_hashkeys = NIL, *outer_hashkeys = NIL;

    foreach(cell, hashclauses) {
        OpExpr *hclause = lfirst_node(OpExpr, cell);
        hashoperators = lappend_oid(hashoperators, hclause->opno);
        hashcollations = lappend_oid(hashcollations, hclause->inputcollid);
        outer_hashkeys = lappend(outer_hashkeys, linitial(hclause->args));
        inner_hashkeys = lappend(inner_hashkeys, lsecond(hclause->args));
    }

    // Create Hash node for inner relation
    Hash *hash_plan = make_hash(inner_plan, inner_hashkeys, skewTable, skewColumn, skewInherit);
    copy_plan_costsize(&hash_plan->plan, inner_plan);
    hash_plan->plan.startup_cost = hash_plan->plan.total_cost;

    // Handle parallel execution
    if (best_path->jpath.path.parallel_aware) {
        hash_plan->plan.parallel_aware = true;
        hash_plan->rows_total = best_path->inner_rows_total;
    }

    // Create HashJoin node
    HashJoin *join_plan = make_hashjoin(tlist, joinclauses, otherclauses, hashclauses,
                                       hashoperators, hashcollations, outer_hashkeys,
                                       outer_plan, (Plan *) hash_plan,
                                       best_path->jpath.jointype, best_path->jpath.inner_unique);

    copy_generic_path_info(&join_plan->join.plan, &best_path->jpath.path);
    return join_plan;
}
```
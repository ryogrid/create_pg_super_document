# process_implied_equality

## Location
[src/backend/optimizer/plan/initsplan.c:2961-3099](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L2961-L3099)

## Overview
Creates a RestrictInfo clause representing an implied equality "item1 op item2" and integrates it into the query planner's constraint system, typically for equivalence class processing.

## Definition

```c
structure with
	 * original (this is necessary in case there are subselects in there...)
	 */
	clause = (Node *) make_opclause(opno,
									BOOLOID,	/* opresulttype */
									false,	/* opretset */
									copyObject(item1),
									copyObject(item2),
									InvalidOid,
									collation);
```
## Detailed Description
This function constructs implied equality clauses that are derived from equivalence class relationships during query optimization. It builds a new OpExpr representing "item1 op item2" (typically a btree equality operator), performs constant folding if both expressions are constant, and integrates the resulting RestrictInfo into the planner's constraint lists.

The function handles several important aspects:
1. **Constant optimization**: When both operands are constants, it attempts to evaluate the expression at planning time
2. **Variable collection**: Identifies all relations referenced by the clause for proper distribution
3. **Pseudoconstant handling**: Manages variable-free clauses by placing them at appropriate join levels
4. **Merge join analysis**: Checks if the clause can be used for merge joins
5. **Target list management**: Ensures variables are available at join nodes

## Parameters / Member Variables
- : PlannerInfo structure containing global planner state
- : OID of the operator (typically a btree equality operator)
- : Collation OID for the comparison operation
- : Left operand expression (copied by the function)
- : Right operand expression (copied by the function)
- : Relids indicating the syntactic scope where clause should apply
- : Security level to assign to the new RestrictInfo
- : Whether both operands are known to be pseudo-constant

## Dependencies
- Functions called/Symbols referenced:
  - [make_opclause](../m/make_opclause.md) (creates operator expression node)
  - copyObject (deep copies expression trees)
  - [eval_const_expressions](../e/eval_const_expressions.md) (performs constant folding)
  - [pull_varnos](pull_varnos.md) (extracts relation IDs from expressions)
  - [bms_is_subset](../b/bms_is_subset.md)/bms_is_empty (bitmap set operations)
  - [get_join_domain_min_rels](../g/get_join_domain_min_rels.md) (determines safe evaluation level)
  - [make_restrictinfo](../m/make_restrictinfo.md) (constructs RestrictInfo node)
  - [bms_membership](../b/bms_membership.md) (checks bitmap set cardinality)
  - [pull_var_clause](pull_var_clause.md) (extracts variable references)
  - [add_vars_to_targetlist](../a/add_vars_to_targetlist.md) (ensures vars available at join)
  - [check_mergejoinable](../c/check_mergejoinable.md) (analyzes merge join suitability)
  - [distribute_restrictinfo_to_rels](../d/distribute_restrictinfo_to_rels.md) (distributes clause to relation lists)

- Called from (representative examples):
  - [generate_base_implied_equalities_const](../g/generate_base_implied_equalities_const.md) (equivalence class constant processing)
  - [generate_base_implied_equalities_no_const](../g/generate_base_implied_equalities_no_const.md) (equivalence class non-constant processing)

## Notes and Other Information
- Returns NULL when constant folding produces TRUE (clause can be eliminated)
- Copies input expressions to avoid sharing substructure with originals
- Handles pseudoconstant clauses by setting appropriate join domain evaluation
- Caller responsible for equivalence class initialization via initialize_mergeclause_eclasses()
- Part of PostgreSQL's equivalence class system for transitive equality inference
- Critical for generating implied join conditions and optimizing complex queries
- Integrates with the broader constraint distribution system through distribute_restrictinfo_to_rels()

## Simplified Source

```c
RestrictInfo *
process_implied_equality(PlannerInfo *root,
                        Oid opno,
                        Oid collation,
                        Expr *item1,
                        Expr *item2,
                        Relids qualscope,
                        Index security_level,
                        bool both_const)
{
    RestrictInfo *restrictinfo;
    Node *clause;
    Relids relids;
    bool pseudoconstant = false;

    // Build equality clause "item1 op item2" with copied operands
    clause = (Node *) make_opclause(opno,
                                    BOOLOID,     /* result type */
                                    false,       /* not set-returning */
                                    copyObject(item1),
                                    copyObject(item2),
                                    InvalidOid,
                                    collation);

    // Try constant folding if both operands are constant
    if (both_const) {
        clause = eval_const_expressions(root, clause);

        // If clause evaluates to TRUE, return NULL (clause can be dropped)
        if (clause && IsA(clause, Const)) {
            Const *cclause = (Const *) clause;
            if (!cclause->constisnull && DatumGetBool(cclause->constvalue))
                return NULL;
        }
    }

    // Find all relations referenced in the clause
    relids = pull_varnos(root, clause);

    // Handle variable-free clauses (pseudoconstants)
    if (bms_is_empty(relids)) {
        relids = get_join_domain_min_rels(root, qualscope);
        pseudoconstant = true;
        root->hasPseudoConstantQuals = true;
    }

    // Create the RestrictInfo node
    restrictinfo = make_restrictinfo(root,
                                    (Expr *) clause,
                                    true,        /* is_pushed_down */
                                    false,       /* !has_clone */
                                    false,       /* !is_clone */
                                    pseudoconstant,
                                    security_level,
                                    relids,
                                    NULL,        /* incompatible_relids */
                                    NULL);       /* outer_relids */

    // For join clauses, ensure variables are in target lists
    if (bms_membership(relids) == BMS_MULTIPLE) {
        List *vars = pull_var_clause(clause,
                                    PVC_RECURSE_AGGREGATES |
                                    PVC_RECURSE_WINDOWFUNCS |
                                    PVC_INCLUDE_PLACEHOLDERS);
        add_vars_to_targetlist(root, vars, relids);
        list_free(vars);
    }

    // Check if clause can be used for merge joins
    check_mergejoinable(restrictinfo);

    // Distribute the clause to appropriate relation lists
    distribute_restrictinfo_to_rels(root, restrictinfo);

    return restrictinfo;
}
```